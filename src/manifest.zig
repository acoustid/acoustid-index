// The manifest records which file segments are live for an index. It's a
// msgpack array of SegmentInfo, written atomically (temp + rename). It is the
// commit point for a checkpoint: a segment is official once it's in the
// manifest.

const std = @import("std");
const zio = @import("zio");
const msgpack = @import("msgpack");
const SegmentInfo = @import("segment.zig").SegmentInfo;

const manifest_file = "manifest";
const manifest_tmp = "manifest.tmp";

/// Read the manifest. Returns an owned slice (caller frees). Missing/empty
/// manifest yields an empty slice.
pub fn read(dir: zio.Dir, allocator: std.mem.Allocator) ![]SegmentInfo {
    const st = dir.statPath(manifest_file) catch |err| switch (err) {
        error.FileNotFound => return &.{},
        else => return err,
    };
    const size: usize = @intCast(st.size);
    if (size == 0) return &.{};

    const file = try dir.openFile(manifest_file, .{ .mode = .read_only });
    defer file.close();

    const buf = try allocator.alloc(u8, size);
    defer allocator.free(buf);
    var off: usize = 0;
    while (off < size) {
        const n = try file.read(buf[off..], off);
        if (n == 0) break;
        off += n;
    }

    var reader = std.Io.Reader.fixed(buf[0..off]);
    return try msgpack.decodeLeaky([]SegmentInfo, allocator, &reader);
}

/// Atomically replace the manifest with `segments`.
pub fn write(dir: zio.Dir, allocator: std.mem.Allocator, segments: []const SegmentInfo) !void {
    var w = std.Io.Writer.Allocating.init(allocator);
    defer w.deinit();
    try msgpack.encode(segments, &w.writer);
    const bytes = w.written();

    const file = try dir.createFile(manifest_tmp, .{ .truncate = true });
    {
        errdefer {
            file.close();
            dir.deleteFile(manifest_tmp) catch {};
        }
        var written: usize = 0;
        while (written < bytes.len) {
            written += try file.write(bytes[written..], written);
        }
        try file.sync(.{});
    }
    file.close();
    try dir.rename(manifest_tmp, dir, manifest_file);
}
