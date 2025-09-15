const std = @import("std");
const log = std.log.scoped(.index_manifest);

const msgpack = @import("msgpack");
const filefmt = @import("filefmt.zig");
const SegmentInfo = @import("segment.zig").SegmentInfo;


const manifest_file_name = "manifest";
const manifest_file_header_magic: u32 = 'I' << 24 | 'D' << 16 | 'X' << 8 | '1';
const max_manifest_file_size: u64 = 1024 * 1024; // 1MB

const ManifestFileHeader = struct {
    magic: u32 = manifest_file_header_magic,
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

pub fn readManifestFile(index_dir: std.fs.Dir, allocator: std.mem.Allocator) ![]SegmentInfo {
    return readManifestFileInternal(index_dir, allocator, manifest_file_name);
}

pub fn readManifestFileBackup(index_dir: std.fs.Dir, allocator: std.mem.Allocator) ![]SegmentInfo {
    return readManifestFileInternal(index_dir, allocator, manifest_file_name ++ ".backup");
}

fn readManifestFileInternal(index_dir: std.fs.Dir, allocator: std.mem.Allocator, file_name: []const u8) ![]SegmentInfo {
    var file = try index_dir.openFile(file_name, .{});
    defer file.close();

    var buffered_reader = std.io.bufferedReader(file.reader());
    const reader = buffered_reader.reader();

    const header_parsed = try msgpack.decode(ManifestFileHeader, allocator, reader);
    defer header_parsed.deinit();

    const header = header_parsed.value;
    if (header.magic != manifest_file_header_magic) {
        return error.WrongManifestFileHeader;
    }

    if (header.size > max_manifest_file_size) {
        return error.ManifestFileTooLarge;
    }

    const data_buffer = try allocator.alloc(u8, header.size);
    defer allocator.free(data_buffer);

    try reader.readNoEof(data_buffer);

    const actual_checksum = std.hash.crc.Crc64Xz.hash(data_buffer);
    if (actual_checksum != header.checksum) {
        return error.ManifestChecksumMismatch;
    }

    var data_stream = std.io.fixedBufferStream(data_buffer);
    return try msgpack.decodeLeaky([]SegmentInfo, allocator, data_stream.reader());
}


pub fn writeManifestFile(index_dir: std.fs.Dir, segments: []const SegmentInfo, allocator: std.mem.Allocator) !void {
    var buffer = std.ArrayList(u8).init(allocator);
    defer buffer.deinit();

    try msgpack.encode(segments, buffer.writer());

    const header = ManifestFileHeader{
        .magic = manifest_file_header_magic,
        .size = buffer.items.len,
        .checksum = std.hash.crc.Crc64Xz.hash(buffer.items),
    };

    var file = try index_dir.atomicFile(manifest_file_name, .{});
    defer file.deinit();

    var buffered_writer = std.io.bufferedWriter(file.file.writer());
    const writer = buffered_writer.writer();

    try msgpack.encode(header, writer);
    try writer.writeAll(buffer.items);
    try buffered_writer.flush();

    try file.file.sync();

    // Create hardlink backup of existing file before finishing
    try filefmt.atomicBackup(index_dir, manifest_file_name, ".backup");

    // Final rename to replace existing file
    try file.finish();

    log.info("wrote index manifest: segments={}", .{segments.len});
}

pub fn encodeManifestData(segments: []const SegmentInfo, writer: anytype, allocator: std.mem.Allocator) !void {
    var buffer = std.ArrayList(u8).init(allocator);
    defer buffer.deinit();

    try msgpack.encode(segments, buffer.writer());

    const header = ManifestFileHeader{
        .magic = manifest_file_header_magic,
        .size = buffer.items.len,
        .checksum = std.hash.crc.Crc64Xz.hash(buffer.items),
    };

    try msgpack.encode(header, writer);
    try writer.writeAll(buffer.items);
}

const testing = std.testing;

test "readManifestFile/writeManifestFile" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const segments = [_]SegmentInfo{
        .{ .version = 1, .merges = 0 },
        .{ .version = 2, .merges = 1 },
        .{ .version = 4, .merges = 0 },
    };

    try writeManifestFile(tmp.dir, &segments, testing.allocator);

    const segments2 = try readManifestFile(tmp.dir, testing.allocator);
    defer testing.allocator.free(segments2);

    try testing.expectEqualSlices(SegmentInfo, &segments, segments2);
}

test "writeManifestFile creates backup" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const segments1 = [_]SegmentInfo{
        .{ .version = 1, .merges = 0 },
    };

    const segments2 = [_]SegmentInfo{
        .{ .version = 1, .merges = 0 },
        .{ .version = 2, .merges = 1 },
    };

    // Write first file
    try writeManifestFile(tmp.dir, &segments1, testing.allocator);

    // Write second file (should create backup of first)
    try writeManifestFile(tmp.dir, &segments2, testing.allocator);

    // Current file should have 2 segments
    const current_segments = try readManifestFile(tmp.dir, testing.allocator);
    defer testing.allocator.free(current_segments);
    try testing.expectEqual(2, current_segments.len);

    // Backup should have 1 segment
    const backup_segments = try readManifestFileBackup(tmp.dir, testing.allocator);
    defer testing.allocator.free(backup_segments);
    try testing.expectEqual(1, backup_segments.len);
    try testing.expectEqual(segments1[0].version, backup_segments[0].version);
}