const std = @import("std");

const IndexReader = @import("IndexReader.zig");
const Index = @import("Index.zig");
const filefmt = @import("filefmt.zig");
const SegmentInfo = @import("segment.zig").SegmentInfo;

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
    try index.waitForReady(1000);

    var hashes: [100]u32 = undefined;
    try index.update(&[_]Change{.{
        .insert = .{
            .id = 1,
            .hashes = generateRandomHashes(&hashes, 1),
        },
    }});

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

    // Restore the snapshot

    var extract_dir = try tmp_dir.dir.makeOpenPath("extract", .{});
    defer extract_dir.close();

    var tar_file_reader = std.io.fixedBufferStream(buffer.items);
    try std.tar.pipeToFileSystem(extract_dir, tar_file_reader.reader(), .{});

    // Open a second index instance from the restored snapshot

    var index2 = try Index.init(std.testing.allocator, &scheduler, tmp_dir.dir, "extract", .{
        .min_segment_size = 1,
    });
    defer index2.deinit();

    try index2.open(true);
    try index2.waitForReady(1000);

    var index_reader2 = try index2.acquireReader();
    defer index2.releaseReader(&index_reader2);

    const i = try index_reader2.getDocInfo(1);
    try std.testing.expect(i != null);
}
