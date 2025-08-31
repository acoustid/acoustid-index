const std = @import("std");
const log = std.log.scoped(.index_redirect);

const msgpack = @import("msgpack");

pub const IndexRedirect = struct {
    name: []const u8, // Logical index name (e.g., "foo.bar")
    version: u64, // Always incremented, even across delete/recreate
    deleted: bool = false, // Deletion flag

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{
            .as_map = .{
                .key = .field_index,
                .omit_defaults = true,
                .omit_nulls = true,
            },
        };
    }

    pub fn msgpackFieldKey(field: std.meta.FieldEnum(@This())) u8 {
        return switch (field) {
            .name => 0x00,
            .version => 0x01,
            .deleted => 0x02,
        };
    }

    pub fn getDataDir(self: IndexRedirect, allocator: std.mem.Allocator) ![]u8 {
        return std.fmt.allocPrint(allocator, "data.{}", .{self.version});
    }

    pub fn init(index_name: []const u8) IndexRedirect {
        return .{
            .name = index_name,
            .version = 0,
            .deleted = true,
        };
    }

    pub fn nextVersion(self: IndexRedirect) IndexRedirect {
        return .{
            .name = self.name,
            .version = self.version + 1,
            .deleted = false,
        };
    }
};

const redirect_file_name = "current";

pub fn readRedirectFile(index_dir: std.fs.Dir, allocator: std.mem.Allocator) !IndexRedirect {
    var file = try index_dir.openFile(redirect_file_name, .{});
    defer file.close();

    var buffered_reader = std.io.bufferedReader(file.reader());
    const reader = buffered_reader.reader();

    return try msgpack.decodeLeaky(IndexRedirect, allocator, reader);
}

pub fn writeRedirectFile(index_dir: std.fs.Dir, redirect: IndexRedirect) !void {
    var file = try index_dir.atomicFile(redirect_file_name, .{});
    defer file.deinit();

    var buffered_writer = std.io.bufferedWriter(file.file.writer());
    const writer = buffered_writer.writer();

    try msgpack.encode(redirect, writer);
    try buffered_writer.flush();

    try file.file.sync();
    try file.finish();

    log.info("wrote redirect: {s} (version={}, deleted={})", .{ redirect.name, redirect.version, redirect.deleted });
}
