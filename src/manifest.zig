// The manifest records which file segments are live for an index. It's a
// msgpack array of SegmentInfo, written atomically (temp + rename). It is the
// commit point for a checkpoint: a segment is official once it's in the
// manifest.

const std = @import("std");
const zio = @import("zio");
const msgpack = @import("msgpack");
const SegmentInfo = @import("segment.zig").SegmentInfo;
const log = std.log.scoped(.manifest);

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

/// Atomically replace the manifest with `segments`. Nothing index-level is stored:
/// whether the index is upstream-fed is derivable from the segments themselves (any
/// non-null SegmentInfo.version), so there is no separate flag to keep in sync.
pub fn write(dir: zio.Dir, segments: []const SegmentInfo) !void {
    const file = try dir.createFile(manifest_tmp, .{ .truncate = true });
    {
        errdefer {
            file.close();
            dir.deleteFileUncancelable(manifest_tmp) catch |err| {
                log.warn("failed to remove temp manifest file: {}", .{err});
            };
        }
        var buf: [4096]u8 = undefined;
        var fw = file.writer(&buf);
        msgpack.encode(segments, &fw.interface) catch |err| switch (err) {
            error.WriteFailed => return fw.err orelse error.Unexpected,
            else => |e| return e,
        };
        fw.interface.flush() catch return fw.err orelse error.Unexpected;
        try file.sync(.{});
    }
    file.close();
    try dir.rename(manifest_tmp, dir, manifest_file);
}
