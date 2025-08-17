const std = @import("std");
const tar_writer = @import("std").tar.writer;

const IndexReader = @import("IndexReader.zig");
const Index = @import("Index.zig");
const filefmt = @import("filefmt.zig");
const SegmentInfo = @import("segment.zig").SegmentInfo;

/// Builds a tar snapshot of the index by adding manifest file, file segments, and WAL files
pub fn buildSnapshot(
    writer: anytype,
    reader: *const IndexReader,
    index: *Index,
    arena: std.mem.Allocator,
) !void {
    // Create tar writer directly on response stream
    var tar_writer_instance = tar_writer(writer);

    // Add manifest file
    try addManifestToSnapshot(&tar_writer_instance, reader, arena);

    // Add file segments
    try addFileSegmentsToSnapshot(&tar_writer_instance, reader, index);

    // Add WAL files
    // FIXME don't add until we make sure it can be made consistent
    // try addWALFilesToSnapshot(&tar_writer_instance, index);
}

fn addManifestToSnapshot(writer: anytype, reader: *const IndexReader, arena: std.mem.Allocator) !void {
    // Collect segment infos from file segments
    var segment_infos = std.ArrayList(SegmentInfo).init(arena);
    defer segment_infos.deinit();

    for (reader.file_segments.value.nodes.items) |node| {
        try segment_infos.append(node.value.info);
    }

    // Serialize manifest to msgpack using proper format with header
    var manifest_data = std.ArrayList(u8).init(arena);
    defer manifest_data.deinit();

    const msgpack_writer = manifest_data.writer();
    try filefmt.encodeManifestData(segment_infos.items, msgpack_writer);

    // Add to tar
    try writer.writeFileBytes(filefmt.manifest_file_name, manifest_data.items, .{});
}

fn addFileSegmentsToSnapshot(writer: anytype, reader: *const IndexReader, index: *Index) !void {
    for (reader.file_segments.value.nodes.items) |node| {
        var filename_buf: [filefmt.max_file_name_size]u8 = undefined;
        const filename = filefmt.buildSegmentFileName(&filename_buf, node.value.info);

        // Open segment file for reading
        var segment_file = try index.dir.openFile(filename, .{});
        defer segment_file.close();

        // Add segment file directly to tar root (no prefix)
        try writer.writeFile(filename, segment_file);
    }
}

fn addWALFilesToSnapshot(writer: anytype, index: *Index) !void {
    // Get WAL directory
    var wal_dir = try index.dir.openDir("oplog", .{ .iterate = true });
    defer wal_dir.close();

    // Iterate through WAL files
    var wal_iterator = wal_dir.iterate();
    while (try wal_iterator.next()) |entry| {
        if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".xlog")) {
            var wal_file = try wal_dir.openFile(entry.name, .{});
            defer wal_file.close();

            // Add to tar with oplog/ prefix
            var tar_path_buf: [128]u8 = undefined;
            const tar_path = try std.fmt.bufPrint(&tar_path_buf, "oplog/{s}", .{entry.name});

            try writer.writeFile(tar_path, wal_file);
        }
    }
}
