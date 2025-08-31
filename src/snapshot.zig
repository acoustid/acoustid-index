const std = @import("std");

const IndexReader = @import("IndexReader.zig");
const Index = @import("Index.zig");
const filefmt = @import("filefmt.zig");
const SegmentInfo = @import("segment.zig").SegmentInfo;

/// Restores an index from a tar snapshot, creating a new index
pub fn restoreSnapshot(
    reader: anytype,
    allocator: std.mem.Allocator,
    scheduler: *@import("utils/Scheduler.zig"),
    parent_dir: std.fs.Dir,
    path: []const u8,
    options: Index.Options,
) !Index {
    // Create index directory
    var extract_dir = try parent_dir.makeOpenPath(path, .{ .iterate = true });
    errdefer parent_dir.deleteTree(path) catch {}; // Clean up directory if anything fails
    defer extract_dir.close();
    
    // Extract tar contents using iterator for pattern matching
    var file_name_buffer: [256]u8 = undefined;
    var link_name_buffer: [256]u8 = undefined;
    var tar_iterator = std.tar.iterator(reader, .{
        .file_name_buffer = &file_name_buffer,
        .link_name_buffer = &link_name_buffer,
    });
    while (try tar_iterator.next()) |entry| {
        // Only extract files we need: manifest and valid segment files
        const is_manifest = std.mem.eql(u8, entry.name, filefmt.manifest_file_name);
        const is_segment = filefmt.parseSegmentFileName(entry.name) != null;
        
        if (is_manifest or is_segment) {
            // Validate path safety - reject absolute paths and path traversal
            if (entry.name[0] == '/' or std.mem.indexOf(u8, entry.name, "..") != null) {
                return error.UnsafePath;
            }
            
            switch (entry.kind) {
                .file => {
                    var file = try extract_dir.createFile(entry.name, .{});
                    defer file.close();
                    var reader_entry = entry.reader();
                    var file_writer = file.writer();
                    
                    var buffer: [4096]u8 = undefined;
                    while (true) {
                        const bytes_read = try reader_entry.read(&buffer);
                        if (bytes_read == 0) break;
                        try file_writer.writeAll(buffer[0..bytes_read]);
                    }
                },
                else => {
                    // Skip directories, symlinks, etc.
                    continue;
                },
            }
        }
    }
    
    // Create and initialize the index
    var index = try Index.init(allocator, scheduler, parent_dir, path, options);
    errdefer index.deinit(); // Clean up index if open fails
    
    // Open the index to load from extracted files
    try index.open(false);
    
    return index;
}

fn cleanupExtractedFiles(dir: std.fs.Dir) void {
    // Remove manifest file if it exists
    dir.deleteFile(filefmt.manifest_file_name) catch {};
    
    // Remove any valid segment files
    var it = dir.iterate();
    while (it.next() catch null) |entry| {
        if (entry.kind == .file and filefmt.parseSegmentFileName(entry.name) != null) {
            dir.deleteFile(entry.name) catch {};
        }
    }
}

/// Builds a tar snapshot of the index by adding manifest file, file segments, and WAL files
pub fn buildSnapshot(
    writer: anytype,
    index: *Index,
    allocator: std.mem.Allocator,
) !void {
    // Acquire reader to get a consistent view of the index
    var index_reader = try index.acquireReader();
    defer index.releaseReader(&index_reader);

    // Create tar writer directly on response stream
    var tar_writer = std.tar.writer(writer);

    // Add manifest file
    try addManifestToSnapshot(&tar_writer, &index_reader, allocator);

    // Add file segments
    try addFileSegmentsToSnapshot(&tar_writer, &index_reader, index);

    // Add empty blocks at the end
    try tar_writer.finish();
}

fn addManifestToSnapshot(writer: anytype, index_reader: *const IndexReader, allocator: std.mem.Allocator) !void {
    // Collect segment infos from file segments
    var segment_infos = std.ArrayList(SegmentInfo).init(allocator);
    defer segment_infos.deinit();

    for (index_reader.file_segments.value.nodes.items) |node| {
        try segment_infos.append(node.value.info);
    }

    // Serialize manifest to msgpack using proper format with header
    var manifest_data = std.ArrayList(u8).init(allocator);
    defer manifest_data.deinit();

    const msgpack_writer = manifest_data.writer();
    try filefmt.encodeManifestData(segment_infos.items, msgpack_writer);

    // Add to tar
    try writer.writeFileBytes(filefmt.manifest_file_name, manifest_data.items, .{});
}

fn addFileSegmentsToSnapshot(writer: anytype, index_reader: *const IndexReader, index: *Index) !void {
    for (index_reader.file_segments.value.nodes.items) |node| {
        var filename_buf: [filefmt.max_file_name_size]u8 = undefined;
        const filename = filefmt.buildSegmentFileName(&filename_buf, node.value.info);

        // Open segment file for reading
        var segment_file = try index.dir.openFile(filename, .{});
        defer segment_file.close();

        // Add segment file directly to tar root (no prefix)
        try writer.writeFile(filename, segment_file);
    }
}

test "index snapshot" {
    const Scheduler = @import("utils/Scheduler.zig");
    const Change = @import("change.zig").Change;
    const generateRandomHashes = @import("index_tests.zig").generateRandomHashes;

    var scheduler = Scheduler.init(std.testing.allocator);
    defer scheduler.deinit();

    try scheduler.start(4);
    defer scheduler.stop();

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var index = try Index.init(std.testing.allocator, &scheduler, tmp_dir.dir, "index", .{
        .min_segment_size = 1, // to trigger checkpoint immediately
    });
    defer index.deinit();

    try index.open(true);

    var hashes: [100]u32 = undefined;
    _ = try index.update(&[_]Change{.{
        .insert = .{
            .id = 1,
            .hashes = generateRandomHashes(&hashes, 1),
        },
    }}, null, null);

    // Wait for checkpoint

    var retries: usize = 0;
    while (true) {
        var index_reader = try index.acquireReader();
        defer index.releaseReader(&index_reader);

        if (index_reader.file_segments.value.count() > 0) {
            break;
        }

        retries += 1;
        try std.testing.expect(retries < 100);

        std.Thread.sleep(std.time.ns_per_ms * 10);
    }

    // Export snapshot

    var buffer = std.ArrayList(u8).init(std.testing.allocator);
    defer buffer.deinit();

    try buildSnapshot(buffer.writer().any(), &index, std.testing.allocator);

    try std.testing.expect(buffer.items.len > 10);

    // Restore the snapshot using restoreSnapshot()

    var tar_file_reader = std.io.fixedBufferStream(buffer.items);
    var index2 = try restoreSnapshot(tar_file_reader.reader(), std.testing.allocator, &scheduler, tmp_dir.dir, "extract", .{
        .min_segment_size = 1,
    });
    defer index2.deinit();

    var index_reader2 = try index2.acquireReader();
    defer index2.releaseReader(&index_reader2);

    const i = try index_reader2.getDocInfo(1);
    try std.testing.expect(i != null);
}

test "restore snapshot with corrupt tar" {
    const Scheduler = @import("utils/Scheduler.zig");

    var scheduler = Scheduler.init(std.testing.allocator);
    defer scheduler.deinit();

    try scheduler.start(4);
    defer scheduler.stop();

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    // Try to restore from corrupt data
    const corrupt_data = "not a tar file";
    var reader = std.io.fixedBufferStream(corrupt_data);
    
    const result = restoreSnapshot(reader.reader(), std.testing.allocator, &scheduler, tmp_dir.dir, "test_corrupt", .{});
    try std.testing.expectError(error.UnexpectedEndOfStream, result);
}

test "restore snapshot cleanup on failure" {
    const Scheduler = @import("utils/Scheduler.zig");

    var scheduler = Scheduler.init(std.testing.allocator);
    defer scheduler.deinit();

    try scheduler.start(4);
    defer scheduler.stop();

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    // Create a valid tar with manifest but no segment files to trigger load failure
    var buffer = std.ArrayList(u8).init(std.testing.allocator);
    defer buffer.deinit();

    var tar_writer = std.tar.writer(buffer.writer().any());
    
    // Add invalid manifest that will cause open() to fail
    const invalid_manifest = "invalid manifest content";
    try tar_writer.writeFileBytes(filefmt.manifest_file_name, invalid_manifest, .{});
    try tar_writer.finish();

    var reader = std.io.fixedBufferStream(buffer.items);
    const result = restoreSnapshot(reader.reader(), std.testing.allocator, &scheduler, tmp_dir.dir, "test_cleanup", .{});
    try std.testing.expectError(error.InvalidFormat, result);

    // Verify cleanup: directory should be removed entirely
    const dir_result = tmp_dir.dir.openDir("test_cleanup", .{});
    try std.testing.expectError(error.FileNotFound, dir_result);
}
