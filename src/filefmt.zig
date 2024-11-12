const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const math = std.math;
const io = std.io;
const fs = std.fs;

const common = @import("common.zig");
const Item = common.Item;
const SegmentVersion = common.SegmentID;
const InMemorySegment = @import("InMemorySegment.zig");
const Segment = @import("Segment.zig");

const msgpack = @import("utils/msgpack/msgpack.zig");

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
const segment_file_name_fmt = "segment-{d}-{d}.dat";
pub const index_file_name = "index.dat";

pub fn buildSegmentFileName(buf: []u8, version: common.SegmentID) []u8 {
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
    var segment = InMemorySegment.init(std.testing.allocator);
    defer segment.deinit();

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

const header_magic_v1: u32 = 0x314D4753; // "SGM1" in little endian
const footer_magic_v1: u32 = @byteSwap(header_magic_v1);

pub const Header = extern struct {
    magic: u32 = header_magic_v1,
    version: u32,
    included_merges: u32,
    max_commit_id: u64,
    block_size: u32,
};

pub const Footer = struct {
    magic: u32,
    num_items: u32,
    num_blocks: u32,
    checksum: u64,
};

const reserved_header_size = 256;
const header_size = @sizeOf(Header);

fn skipPadding(reader: anytype, reserved: comptime_int, used: comptime_int) !void {
    const padding_size = reserved - used;
    try reader.skipBytes(padding_size, .{});
}

fn writePadding(writer: anytype, reserved: comptime_int, used: comptime_int) !void {
    const padding_size = reserved - used;
    const padding = &[_]u8{0} ** padding_size;
    try writer.writeAll(padding);
}

pub fn readHeader(reader: anytype) !Header {
    const header = try reader.readStructEndian(Header, .little);
    try skipPadding(reader, reserved_header_size, header_size);
    return header;
}

pub fn writeHeader(writer: anytype, header: Header) !void {
    assert(header.magic == header_magic_v1);
    try writer.writeStructEndian(header, .little);
    try writePadding(writer, reserved_header_size, header_size);
}

const DocInfo = packed struct(u64) {
    id: u32,
    version: u24,
    deleted: u8,
};

pub fn writeFile(file: std.fs.File, reader: anytype) !void {
    const block_size = default_block_size;

    var buffered_writer = std.io.bufferedWriter(file.writer());
    var counting_writer = std.io.countingWriter(buffered_writer.writer());
    const writer = counting_writer.writer();

    const packer = msgpack.packer(writer, .{});

    const segment = reader.segment;

    const header = Header{
        .version = segment.id.version,
        .included_merges = segment.id.included_merges,
        .max_commit_id = segment.max_commit_id,
        .block_size = block_size,
    };
    try packer.write(Header, header);

    try packer.writeMapHeader(segment.docs.count());
    var docs_iter = segment.docs.iterator();
    while (docs_iter.next()) |entry| {
        try packer.write(u32, entry.key_ptr.*);
        try packer.write(bool, entry.value_ptr.*);
    }

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

    const footer = Footer{
        .magic = footer_magic_v1,
        .num_items = num_items,
        .num_blocks = num_blocks,
        .checksum = crc.final(),
    };
    try packer.write(Footer, footer);

    try buffered_writer.flush();

    try file.sync();
}

pub fn readFile(file: fs.File, segment: *Segment) !void {
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

    const unpacker = msgpack.unpackerNoAlloc(reader, .{});

    const header = try unpacker.read(Header);

    if (header.magic != header_magic_v1) {
        return error.InvalidSegment;
    }
    if (header.block_size < min_block_size or header.block_size > max_block_size) {
        return error.InvalidSegment;
    }

    segment.id.version = header.version;
    segment.id.included_merges = header.included_merges;
    segment.block_size = header.block_size;
    segment.max_commit_id = header.max_commit_id;

    const num_docs = try unpacker.readMapHeader(.required);
    try segment.docs.ensureTotalCapacity(num_docs);
    for (0..num_docs) |_| {
        const key = try unpacker.read(u32);
        const value = try unpacker.read(bool);
        try segment.docs.put(key, value);
    }

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

    const footer = try unpacker.read(Footer);
    if (footer.magic != footer_magic_v1) {
        return error.InvalidSegment;
    }
    if (footer.num_items != num_items) {
        return error.InvalidSegment;
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

    {
        var file = try tmp.dir.createFile("test.dat", .{});
        defer file.close();

        var in_memory_segment = InMemorySegment.init(testing.allocator);
        defer in_memory_segment.deinit();

        in_memory_segment.id.version = 1;

        try in_memory_segment.build(&.{
            .{ .insert = .{ .id = 1, .hashes = &[_]u32{ 1, 2 } } },
        });

        var reader = in_memory_segment.reader();
        defer reader.close();

        try writeFile(file, &reader);
    }

    {
        var file = try tmp.dir.openFile("test.dat", .{});
        defer file.close();

        var segment = Segment.init(testing.allocator);
        defer segment.deinit();

        try readFile(file, &segment);

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

const index_header_magic_v1: u32 = 0x31584449; // "IDX1" in little endian

const IndexHeader = extern struct {
    magic: u32,
};

pub fn writeIndexFile(writer: anytype, segments: std.ArrayList(SegmentVersion)) !void {
    const packer = msgpack.packer(writer, .{});

    const header = IndexHeader{
        .magic = index_header_magic_v1,
    };
    try packer.writeStruct(IndexHeader, header, 1);

    try packer.writeArrayHeader(segments.items.len);
    for (segments.items) |segment| {
        try packer.writeArrayHeader(2);
        try packer.writeInt(u32, segment.version);
        try packer.writeInt(u32, segment.included_merges);
    }
}

pub fn readIndexFile(reader: anytype, segments: *std.ArrayList(SegmentVersion)) !void {
    const unpacker = msgpack.unpackerNoAlloc(reader, .{});

    const header = try unpacker.readStruct(IndexHeader, .required, 1);
    if (header.magic != index_header_magic_v1) {
        return error.InvalidIndexfile;
    }

    const num_segments = try unpacker.readArrayHeader(.required);
    try segments.ensureTotalCapacityPrecise(num_segments);

    for (0..num_segments) |_| {
        const num_fields = try unpacker.readArrayHeader(.required);
        if (num_fields != 2) {
            return error.InvalidIndexfile;
        }
        const version = try unpacker.readInt(u32);
        const included_merges = try unpacker.readInt(u32);
        try segments.append(.{ .version = version, .included_merges = included_merges });
    }
}

test "readIndexFile/writeIndexFile" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var segments = std.ArrayList(SegmentVersion).init(testing.allocator);
    defer segments.deinit();

    try segments.append(.{ .version = 1, .included_merges = 0 });
    try segments.append(.{ .version = 2, .included_merges = 1 });
    try segments.append(.{ .version = 4, .included_merges = 0 });

    {
        var file = try tmp.dir.createFile("test.idx", .{});
        defer file.close();

        try writeIndexFile(file.writer(), segments);
    }

    {
        var file = try tmp.dir.openFile("test.idx", .{});
        defer file.close();

        var segments2 = std.ArrayList(SegmentVersion).init(testing.allocator);
        defer segments2.deinit();

        try readIndexFile(file.reader(), &segments2);

        try testing.expectEqualSlices(SegmentVersion, segments.items, segments2.items);
    }
}
