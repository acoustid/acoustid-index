const std = @import("std");
const log = std.log.scoped(.index_redirect);

const msgpack = @import("msgpack");

pub const IndexRedirect = struct {
    name: []const u8, // Logical index name (e.g., "foo.bar")
    version: u64, // Always incremented, even across delete/recreate
    deleted: bool = false, // Deletion flag

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .custom } };
    }

    pub fn msgpackFieldKey(field: std.meta.FieldEnum(@This())) u8 {
        return switch (field) {
            .name => 1,
            .version => 2,
            .deleted => 3,
        };
    }

    pub fn getDataDir(self: IndexRedirect, allocator: std.mem.Allocator) ![]u8 {
        return std.fmt.allocPrint(allocator, "data.{}", .{self.version});
    }

    pub fn init(index_name: []const u8) IndexRedirect {
        return .{
            .name = index_name,
            .version = 1,
            .deleted = false,
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

const index_redirect_file_name = "current";
const index_redirect_file_header_magic: u32 = 'I' << 24 | 'R' << 16 | 'D' << 8 | '1';
const max_redirect_file_size: u64 = 64 * 1024; // 64KB

const IndexRedirectFileHeader = struct {
    magic: u32 = index_redirect_file_header_magic,
    size: u64,
    checksum: u64,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .custom } };
    }

    pub fn msgpackFieldKey(field: std.meta.FieldEnum(@This())) u8 {
        return switch (field) {
            .magic => 1,
            .size => 2,
            .checksum => 3,
        };
    }
};

pub fn readRedirectFile(index_dir: std.fs.Dir, allocator: std.mem.Allocator) !IndexRedirect {
    var file = try index_dir.openFile(index_redirect_file_name, .{});
    defer file.close();

    var buffered_reader = std.io.bufferedReader(file.reader());
    const reader = buffered_reader.reader();

    const header_parsed = try msgpack.decode(IndexRedirectFileHeader, allocator, reader);
    defer header_parsed.deinit();

    const header = header_parsed.value;
    if (header.magic != index_redirect_file_header_magic) {
        return error.WrongIndexRedirectFileHeader;
    }

    if (header.size > max_redirect_file_size) {
        return error.IndexRedirectFileTooLarge;
    }

    const data_buffer = try allocator.alloc(u8, header.size);
    defer allocator.free(data_buffer);

    try reader.readNoEof(data_buffer);

    const actual_checksum = std.hash.crc.Crc64Xz.hash(data_buffer);
    if (actual_checksum != header.checksum) {
        return error.IndexRedirectChecksumMismatch;
    }

    var data_stream = std.io.fixedBufferStream(data_buffer);
    return try msgpack.decodeLeaky(IndexRedirect, allocator, data_stream.reader());
}

pub fn atomicBackup(dir: std.fs.Dir, comptime src: []const u8, comptime suffix: []const u8) !void {
    const dest = src ++ suffix;
    const tmp_dest = dest ++ ".tmp";
    std.posix.linkat(dir.fd, src, dir.fd, tmp_dest, 0) catch |err| switch (err) {
        error.FileNotFound => return, // File not found, nothing to backup
        error.PathAlreadyExists => {
            // Found existing file, delete it
            try std.posix.unlinkat(dir.fd, tmp_dest, 0);
            try std.posix.linkat(dir.fd, src, dir.fd, tmp_dest, 0);
        },
        else => return err,
    };
    try std.posix.renameat(dir.fd, tmp_dest, dir.fd, dest);
}

pub fn writeRedirectFile(index_dir: std.fs.Dir, redirect: IndexRedirect, allocator: std.mem.Allocator) !void {
    var buffer = std.ArrayList(u8).init(allocator);
    defer buffer.deinit();

    try msgpack.encode(redirect, buffer.writer());

    const header = IndexRedirectFileHeader{
        .magic = index_redirect_file_header_magic,
        .size = buffer.items.len,
        .checksum = std.hash.crc.Crc64Xz.hash(buffer.items),
    };

    var file = try index_dir.atomicFile(index_redirect_file_name, .{});
    defer file.deinit();

    var buffered_writer = std.io.bufferedWriter(file.file.writer());
    const writer = buffered_writer.writer();

    try msgpack.encode(header, writer);
    try writer.writeAll(buffer.items);
    try buffered_writer.flush();

    try file.file.sync();

    // Create hardlink backup of existing file before finishing
    try atomicBackup(index_dir, index_redirect_file_name, ".backup");

    // Final rename to replace existing file
    try file.finish();

    log.info("wrote index redirect: {s} (version={}, deleted={})", .{ redirect.name, redirect.version, redirect.deleted });
}
