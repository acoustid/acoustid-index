const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const math = std.math;
const io = std.io;
const fs = std.fs;
const log = std.log.scoped(.filefmt);

const msgpack = @import("msgpack");

const common = @import("common.zig");
const Item = common.Item;
const SegmentId = common.SegmentId;
const MemorySegment = @import("MemorySegment.zig");
const FileSegment = @import("FileSegment.zig");

pub const default_block_size = 1024;
pub const min_block_size = 256;
pub const max_block_size = 4096;

pub fn maxItemsPerBlock(block_size: usize) usize {
    return (block_size - 2) / (2 * min_varint32_size);
}

const min_varint32_size = 1;
const max_varint32_size = 5;

fn varint32Size(value: u32) usize {
    if (value < (1 << 7)) {
        return 1;
    }
    if (value < (1 << 14)) {
        return 2;
    }
    if (value < (1 << 21)) {
        return 3;
    }
    if (value < (1 << 28)) {
        return 4;
    }
    return max_varint32_size;
}

test "check varint32Size" {
    try testing.expectEqual(1, varint32Size(1));
    try testing.expectEqual(2, varint32Size(1000));
    try testing.expectEqual(3, varint32Size(100000));
    try testing.expectEqual(4, varint32Size(10000000));
    try testing.expectEqual(5, varint32Size(1000000000));
    try testing.expectEqual(5, varint32Size(math.maxInt(u32)));
}

fn writeVarint32(buf: []u8, value: u32) usize {
    assert(buf.len >= varint32Size(value));
    var v = value;
    var i: usize = 0;
    while (i < max_varint32_size) : (i += 1) {
        buf[i] = @intCast(v & 0x7F);
        v >>= 7;
        if (v == 0) {
            return i + 1;
        }
        buf[i] |= 0x80;
    }
    unreachable;
}

fn readVarint32(buf: []const u8) struct { value: u32, size: usize } {
    var v: u32 = 0;
    var shift: u5 = 0;
    var i: usize = 0;
    while (i < @min(max_varint32_size, buf.len)) : (i += 1) {
        const b = buf[i];
        v |= @as(u32, @intCast(b & 0x7F)) << shift;
        if (b & 0x80 == 0) {
            return .{ .value = v, .size = i + 1 };
        }
        shift += 7;
    }
    return .{ .value = v, .size = i };
}

test "check writeVarint32" {
    var buf: [max_varint32_size]u8 = undefined;

    try std.testing.expectEqual(1, writeVarint32(&buf, 1));
    try std.testing.expectEqualSlices(u8, &[_]u8{0x01}, buf[0..1]);

    try std.testing.expectEqual(2, writeVarint32(&buf, 1000));
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xe8, 0x07 }, buf[0..2]);
}

pub const max_file_name_size = 64;
const segment_file_name_fmt = "segment-{x:0>8}-{x:0>8}.dat";
pub const index_file_name = "index.dat";

pub fn buildSegmentFileName(buf: []u8, version: common.SegmentId) []u8 {
    assert(buf.len == max_file_name_size);
    return std.fmt.bufPrint(buf, segment_file_name_fmt, .{ version.version, version.version + version.included_merges }) catch unreachable;
}

const BlockHeader = struct {
    num_items: u16,
    first_item: Item,
};

pub fn decodeBlockHeader(data: []const u8) !BlockHeader {
    assert(data.len >= min_block_size);

    const num_items = std.mem.readInt(u16, data[0..2], .little);
    if (num_items == 0) {
        return .{ .num_items = 0, .first_item = .{ .hash = 0, .id = 0 } };
    }

    var ptr: usize = 2;
    const hash = readVarint32(data[ptr..]);
    ptr += hash.size;
    const id = readVarint32(data[ptr..]);
    ptr += id.size;

    return .{
        .num_items = num_items,
        .first_item = Item{ .hash = hash.value, .id = id.value },
    };
}

pub fn readBlock(data: []const u8, items: *std.ArrayList(Item)) !void {
    var ptr: usize = 0;

    if (data.len < 2) {
        return error.InvalidBlock;
    }

    const total_items = std.mem.readInt(u16, data[0..2], .little);
    ptr += 2;

    try items.ensureUnusedCapacity(total_items);

    var last_hash: u32 = 0;
    var last_doc_id: u32 = 0;

    var num_items: u16 = 0;
    while (num_items < total_items) {
        if (ptr + 2 * min_varint32_size > data.len) {
            return error.InvalidBlock;
        }
        const diff_hash = readVarint32(data[ptr..]);
        ptr += diff_hash.size;
        const diff_doc_id = readVarint32(data[ptr..]);
        ptr += diff_doc_id.size;

        last_hash += diff_hash.value;
        last_doc_id = if (diff_hash.value > 0) diff_doc_id.value else last_doc_id + diff_doc_id.value;

        const item = items.addOneAssumeCapacity();
        item.* = .{ .hash = last_hash, .id = last_doc_id };
        num_items += 1;
    }

    if (num_items < total_items) {
        return error.InvalidBlock;
    }
}

pub fn encodeBlock(data: []u8, reader: anytype) !u16 {
    assert(data.len >= 2);

    var ptr: usize = 2;
    var num_items: u16 = 0;
    var last_hash: u32 = 0;
    var last_doc_id: u32 = 0;

    while (true) {
        const item = try reader.read() orelse break;
        assert(item.hash > last_hash or (item.hash == last_hash and item.id >= last_doc_id));

        const diff_hash = item.hash - last_hash;
        const diff_doc_id = if (diff_hash > 0) item.id else item.id - last_doc_id;

        if (ptr + varint32Size(diff_hash) + varint32Size(diff_doc_id) > data.len) {
            break;
        }

        ptr += writeVarint32(data[ptr..], diff_hash);
        ptr += writeVarint32(data[ptr..], diff_doc_id);

        last_hash = item.hash;
        last_doc_id = item.id;

        num_items += 1;
        reader.advance();
    }

    std.mem.writeInt(u16, data[0..2], num_items, .little);
    @memset(data[ptr..], 0);

    return num_items;
}

test "writeBlock/readBlock/readFirstItemFromBlock" {
    var segment = MemorySegment.init(std.testing.allocator, .{});
    defer segment.deinit(.delete);

    try segment.items.append(.{ .hash = 1, .id = 1 });
    try segment.items.append(.{ .hash = 2, .id = 1 });
    try segment.items.append(.{ .hash = 3, .id = 1 });
    try segment.items.append(.{ .hash = 3, .id = 2 });
    try segment.items.append(.{ .hash = 4, .id = 1 });

    const block_size = 1024;
    var block_data: [block_size]u8 = undefined;

    var reader = segment.reader();
    const num_items = try encodeBlock(block_data[0..], &reader);
    try testing.expectEqual(segment.items.items.len, num_items);

    var items = std.ArrayList(Item).init(std.testing.allocator);
    defer items.deinit();

    try readBlock(block_data[0..], &items);
    try testing.expectEqualSlices(
        Item,
        &[_]Item{
            .{ .hash = 1, .id = 1 },
            .{ .hash = 2, .id = 1 },
            .{ .hash = 3, .id = 1 },
            .{ .hash = 3, .id = 2 },
            .{ .hash = 4, .id = 1 },
        },
        items.items,
    );

    const header = try decodeBlockHeader(block_data[0..]);
    try testing.expectEqual(items.items.len, header.num_items);
    try testing.expectEqual(items.items[0], header.first_item);
}

const segment_file_header_magic_v1: u32 = 0x53474D31; // "SGM1" in big endian
const segment_file_footer_magic_v1: u32 = @byteSwap(segment_file_header_magic_v1);

pub const SegmentFileHeader = extern struct {
    magic: u32 = segment_file_header_magic_v1,
    version: u32,
    included_merges: u32,
    max_commit_id: u64,
    block_size: u32,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{
            .as_map = .{
                .key = .field_index,
                .omit_defaults = false,
                .omit_nulls = true,
            },
        };
    }
};

pub const SegmentFileFooter = struct {
    magic: u32 = segment_file_footer_magic_v1,
    num_items: u32,
    num_blocks: u32,
    checksum: u64,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{
            .as_map = .{
                .key = .field_index,
                .omit_defaults = false,
                .omit_nulls = true,
            },
        };
    }
};

pub fn deleteSegmentFile(dir: std.fs.Dir, segment_id: SegmentId) !void {
    var file_name_buf: [max_file_name_size]u8 = undefined;
    const file_name = buildSegmentFileName(&file_name_buf, segment_id);

    log.info("deleting segment file {s}", .{file_name});

    try dir.deleteFile(file_name);
}

pub fn writeSegmentFile(dir: std.fs.Dir, reader: anytype) !void {
    const segment = reader.segment;

    var file_name_buf: [max_file_name_size]u8 = undefined;
    const file_name = buildSegmentFileName(&file_name_buf, segment.id);

    log.info("writing segment file {s}", .{file_name});

    var file = try dir.atomicFile(file_name, .{});
    errdefer file.deinit();

    const block_size = default_block_size;

    var buffered_writer = std.io.bufferedWriter(file.file.writer());
    var counting_writer = std.io.countingWriter(buffered_writer.writer());
    const writer = counting_writer.writer();

    const packer = msgpack.packer(writer);

    const header = SegmentFileHeader{
        .version = segment.id.version,
        .included_merges = segment.id.included_merges,
        .max_commit_id = segment.max_commit_id,
        .block_size = block_size,
    };
    try packer.write(SegmentFileHeader, header);

    try packer.writeMap(segment.docs);

    try buffered_writer.flush();

    const padding_size = block_size - counting_writer.bytes_written % block_size;
    try writer.writeByteNTimes(0, padding_size);

    var num_items: u32 = 0;
    var num_blocks: u32 = 0;
    var crc = std.hash.crc.Crc64Xz.init();

    var block_data: [block_size]u8 = undefined;
    while (true) {
        const n = try encodeBlock(block_data[0..], reader);
        try writer.writeAll(block_data[0..]);
        if (n == 0) {
            break;
        }
        num_items += n;
        num_blocks += 1;
        crc.update(block_data[0..]);
    }

    const footer = SegmentFileFooter{
        .magic = segment_file_footer_magic_v1,
        .num_items = num_items,
        .num_blocks = num_blocks,
        .checksum = crc.final(),
    };
    try packer.write(SegmentFileFooter, footer);

    try buffered_writer.flush();

    try file.file.sync();

    try file.finish();

    log.info("wrote segment file {s} (blocks = {}, items = {}, checksum = {})", .{
        file_name,
        footer.num_blocks,
        footer.num_items,
        footer.checksum,
    });
}

pub fn readSegmentFile(dir: fs.Dir, id: SegmentId, segment: *FileSegment) !void {
    var file_name_buf: [max_file_name_size]u8 = undefined;
    const file_name = buildSegmentFileName(&file_name_buf, id);

    log.info("reading segment file {s}", .{file_name});

    var file = try dir.openFile(file_name, .{});
    errdefer file.close();

    const file_size = try file.getEndPos();

    var raw_data = try std.posix.mmap(
        null,
        file_size,
        std.posix.PROT.READ,
        .{ .TYPE = .PRIVATE, .POPULATE = true },
        file.handle,
        0,
    );
    segment.raw_data = raw_data;

    try std.posix.madvise(
        raw_data.ptr,
        raw_data.len,
        std.posix.MADV.RANDOM | std.posix.MADV.WILLNEED,
    );

    var fixed_buffer_stream = std.io.fixedBufferStream(raw_data[0..]);
    const reader = fixed_buffer_stream.reader();

    const unpacker = msgpack.unpacker(reader, null);

    const header = try unpacker.read(SegmentFileHeader);

    if (header.magic != segment_file_header_magic_v1) {
        return error.InvalidSegment;
    }
    if (header.block_size < min_block_size or header.block_size > max_block_size) {
        return error.InvalidSegment;
    }

    segment.id.version = header.version;
    segment.id.included_merges = header.included_merges;
    segment.block_size = header.block_size;
    segment.max_commit_id = header.max_commit_id;

    try unpacker.readMapInto(&segment.docs);

    const block_size = header.block_size;
    const padding_size = block_size - fixed_buffer_stream.pos % block_size;
    try reader.skipBytes(padding_size, .{});

    const blocks_data_start = fixed_buffer_stream.pos;

    const estimated_block_count = (raw_data.len - fixed_buffer_stream.pos) / block_size;
    try segment.index.ensureTotalCapacity(estimated_block_count);

    var num_items: u32 = 0;
    var num_blocks: u32 = 0;
    var crc = std.hash.crc.Crc64Xz.init();

    var block_data_buffer: [max_block_size]u8 = undefined;
    var block_data = block_data_buffer[0..block_size];
    while (true) {
        try reader.readNoEof(block_data);
        const block_header = try decodeBlockHeader(block_data);
        if (block_header.num_items == 0) {
            break;
        }
        try segment.index.append(block_header.first_item.hash);
        num_items += block_header.num_items;
        num_blocks += 1;
        crc.update(block_data[0..]);
    }

    const blocks_data_end = fixed_buffer_stream.pos;
    segment.blocks = raw_data[blocks_data_start..blocks_data_end];

    const footer = try unpacker.read(SegmentFileFooter);
    if (footer.magic != segment_file_footer_magic_v1) {
        return error.InvalidSegment;
    }
    if (footer.num_items != num_items) {
        return error.InvalidSegment;
    } else {
        segment.num_items = num_items;
    }
    if (footer.num_blocks != num_blocks) {
        return error.InvalidSegment;
    }
    if (footer.checksum != crc.final()) {
        return error.InvalidSegment;
    }
}

test "writeFile/readFile" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const version: SegmentId = .{ .version = 1, .included_merges = 0 };

    {
        var in_memory_segment = MemorySegment.init(testing.allocator, .{});
        defer in_memory_segment.deinit(.delete);

        in_memory_segment.id = version;

        try in_memory_segment.build(&.{
            .{ .insert = .{ .id = 1, .hashes = &[_]u32{ 1, 2 } } },
        });

        var reader = in_memory_segment.reader();
        defer reader.close();

        try writeSegmentFile(tmp.dir, &reader);
    }

    {
        var segment = FileSegment.init(testing.allocator, .{ .dir = tmp.dir });
        defer segment.deinit(.delete);

        try readSegmentFile(tmp.dir, version, &segment);

        try testing.expectEqual(1, segment.id.version);
        try testing.expectEqual(0, segment.id.included_merges);
        try testing.expectEqual(1, segment.docs.count());
        try testing.expectEqual(1, segment.index.items.len);
        try testing.expectEqual(1, segment.index.items[0]);

        var items = std.ArrayList(Item).init(testing.allocator);
        defer items.deinit();

        try readBlock(segment.getBlockData(0), &items);
        try std.testing.expectEqualSlices(Item, &[_]Item{
            Item{ .hash = 1, .id = 1 },
            Item{ .hash = 2, .id = 1 },
        }, items.items);
    }
}

const index_header_magic_v1: u32 = 0x49445831; // "IDX1" in big endian

const IndexFileHeader = struct {
    magic: u32 = index_header_magic_v1,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .field_index } };
    }
};

pub fn writeIndexFile(dir: std.fs.Dir, segments: []const SegmentId) !void {
    log.info("writing index file {s}", .{index_file_name});

    var file = try dir.atomicFile(index_file_name, .{});
    defer file.deinit();

    var buffered_writer = std.io.bufferedWriter(file.file.writer());
    const writer = buffered_writer.writer();

    try msgpack.encode(IndexFileHeader{}, writer);
    try msgpack.encode(segments, writer);

    try buffered_writer.flush();

    try file.file.sync();

    try file.finish();

    log.info("wrote index file {s} (segments = {})", .{
        index_file_name,
        segments.len,
    });
}

pub fn readIndexFile(dir: std.fs.Dir, allocator: std.mem.Allocator) ![]SegmentId {
    log.info("reading index file {s}", .{index_file_name});

    var file = try dir.openFile(index_file_name, .{});
    defer file.close();

    var buffered_reader = std.io.bufferedReader(file.reader());
    const reader = buffered_reader.reader();

    const header = try msgpack.decodeLeaky(IndexFileHeader, null, reader);
    if (header.magic != index_header_magic_v1) {
        return error.InvalidIndexfile;
    }

    return try msgpack.decodeLeaky([]SegmentId, allocator, reader);
}

test "readIndexFile/writeIndexFile" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const segments = [_]SegmentId{
        .{ .version = 1, .included_merges = 0 },
        .{ .version = 2, .included_merges = 1 },
        .{ .version = 4, .included_merges = 0 },
    };

    try writeIndexFile(tmp.dir, &segments);

    const segments2 = try readIndexFile(tmp.dir, std.testing.allocator);
    defer std.testing.allocator.free(segments2);

    try testing.expectEqualSlices(SegmentId, &segments, segments2);
}
