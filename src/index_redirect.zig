// An index directory (data/<name>/) holds a `current` redirect file recording
// the index's generation and whether it's deleted; the actual index data lives in
// a generation-scoped subdir `v<generation>`. Generation is always incremented,
// even across delete/recreate, so each lineage is a physically separate directory
// and the redirect makes the current generation durable — that's the reconcile
// key: a node compares its persisted generation to the meta feed's create.pos.
//
// In standalone mode generation is incremented locally (nextGeneration()); in
// replicated mode it is the coordinator's meta-op position. Either way it lands
// here and in the `v<generation>` subdir name.

const std = @import("std");
const zio = @import("zio");
const msgpack = @import("msgpack");
const log = std.log.scoped(.index_redirect);

pub const max_data_dir_len = 24; // "v" + up to 20 digits + slack

pub const IndexRedirect = struct {
    name: []const u8,
    generation: u64,
    deleted: bool = false,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }

    /// The generation-scoped subdirectory holding this lineage's index data.
    /// Zero-padded so it reads/sorts nicely; never truncates (min width 6).
    pub fn dataDir(self: IndexRedirect, buf: []u8) []const u8 {
        return std.fmt.bufPrint(buf, "v{d:0>6}", .{self.generation}) catch unreachable;
    }

    pub fn nextGeneration(self: IndexRedirect) IndexRedirect {
        return .{ .name = self.name, .generation = self.generation + 1, .deleted = false };
    }
};

const redirect_file = "current";
const redirect_tmp = "current.tmp";

/// Read the redirect from an index dir. Returns an owned `name` (caller frees).
/// Propagates error.FileNotFound when the dir has no redirect yet.
pub fn read(dir: zio.Dir, allocator: std.mem.Allocator) !IndexRedirect {
    const st = try dir.statPath(redirect_file);
    const size: usize = @intCast(st.size);

    const file = try dir.openFile(redirect_file, .{ .mode = .read_only });
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
    return try msgpack.decodeLeaky(IndexRedirect, allocator, &reader);
}

/// Atomically replace the redirect (temp + rename).
pub fn write(dir: zio.Dir, allocator: std.mem.Allocator, redirect: IndexRedirect) !void {
    var w = std.Io.Writer.Allocating.init(allocator);
    defer w.deinit();
    try msgpack.encode(redirect, &w.writer);
    const bytes = w.written();

    const file = try dir.createFile(redirect_tmp, .{ .truncate = true });
    {
        errdefer {
            file.close();
            zio.beginShield();
            defer zio.endShield();
            dir.deleteFile(redirect_tmp) catch |err| {
                log.warn("failed to remove temp redirect file: {}", .{err});
            };
        }
        var written: usize = 0;
        while (written < bytes.len) {
            written += try file.write(bytes[written..], written);
        }
        try file.sync(.{});
    }
    file.close();
    try dir.rename(redirect_tmp, dir, redirect_file);
}

// ---- tests ----

const testing = std.testing;

test "IndexRedirect.dataDir zero-pads and never truncates" {
    var buf: [max_data_dir_len]u8 = undefined;
    try testing.expectEqualStrings("v000042", (IndexRedirect{ .name = "x", .generation = 42 }).dataDir(&buf));
    try testing.expectEqualStrings("v123456789", (IndexRedirect{ .name = "x", .generation = 123456789 }).dataDir(&buf));
}

test "IndexRedirect.nextGeneration" {
    const r = (IndexRedirect{ .name = "x", .generation = 5 }).nextGeneration();
    try testing.expectEqual(@as(u64, 6), r.generation);
    try testing.expectEqual(false, r.deleted);
}

test "write/read round-trip" {
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_index_redirect";
    cwd.createDir(dir_path, 0o755) catch {};
    var dir = try cwd.openDir(dir_path, .{ .iterate = true });
    defer {
        dir.deleteFile(redirect_file) catch {};
        dir.close();
        cwd.deleteDir(dir_path) catch {};
    }

    try write(dir, testing.allocator, .{ .name = "test.index", .generation = 123, .deleted = false });
    const r = try read(dir, testing.allocator);
    defer testing.allocator.free(r.name);
    try testing.expectEqualStrings("test.index", r.name);
    try testing.expectEqual(@as(u64, 123), r.generation);
    try testing.expectEqual(false, r.deleted);
}
