const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const math = std.math;
const io = std.io;
const fs = std.fs;

const Item = @import("common.zig").Item;
const InMemorySegment = @import("InMemorySegment.zig");
const Segment = @import("Segment.zig");

pub const default_block_size = 1024;
pub const min_block_size = 256;
pub const max_block_size = 4096;

const minVarint32Size = 1;
const maxVarint32Size = 5;

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
    return maxVarint32Size;
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
    while (i < maxVarint32Size) : (i += 1) {
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
    while (i < @min(maxVarint32Size, buf.len)) : (i += 1) {
        const b = buf[i];
        v |= @as(u32, @intCast(b & 0x7F)) << shift;
        shift += 7;
        if (b & 0x80 == 0) {
            return .{ .value = v, .size = i + 1 };
        }
    }
    return .{ .value = v, .size = i };
}

test "check writeVarint32" {
    var buf: [maxVarint32Size]u8 = undefined;

    try std.testing.expectEqual(1, writeVarint32(&buf, 1));
    try std.testing.expectEqualSlices(u8, &[_]u8{0x01}, buf[0..1]);

    try std.testing.expectEqual(2, writeVarint32(&buf, 1000));
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xe8, 0x07 }, buf[0..2]);
}

pub fn minItemsPerBlock(blockSize: usize) usize {
    return (blockSize - 4) / 2;
}

pub fn readFirstItemFromBlock(data: []const u8) !Item {
    if (data.len < 2 + minVarint32Size * 2) {
        return error.InvalidBlock;
    }
    const num_items = std.mem.readInt(u16, data[0..2], .little);
    if (num_items == 0) {
        return error.InvalidBlock;
    }
    var ptr: usize = 2;
    const hash = readVarint32(data[ptr..]);
    ptr += hash.size;
    const id = readVarint32(data[ptr..]);
    ptr += id.size;
    return Item{ .hash = hash.value, .id = id.value };
}

pub fn readBlock(data: []const u8, items: *std.ArrayList(Item)) !void {
    var ptr: usize = 0;

    if (data.len < 2) {
        return error.InvalidBlock;
    }

    const numItems = std.mem.readInt(u16, data[0..2], .little);
    ptr += 2;

    items.clearRetainingCapacity();
    try items.ensureTotalCapacity(numItems);

    var lastHash: u32 = 0;
    var lastDocId: u32 = 0;

    while (ptr + 2 * minVarint32Size < data.len) {
        const diffHash = readVarint32(data[ptr..]);
        ptr += diffHash.size;
        const diffDocId = readVarint32(data[ptr..]);
        ptr += diffDocId.size;

        lastHash += diffHash.value;
        lastDocId = if (diffHash.value > 0) diffDocId.value else lastDocId + diffDocId.value;

        try items.append(.{ .hash = lastHash, .id = lastDocId });

        if (items.items.len >= numItems) {
            break;
        }
    }

    if (items.items.len < numItems) {
        return error.InvalidBlock;
    }
}

pub fn writeBlock(data: []u8, items: []const Item) !usize {
    assert(data.len >= 2);

    var ptr: usize = 2;
    var numItems: u16 = 0;
    var lastHash: u32 = 0;
    var lastDocId: u32 = 0;

    for (items) |item| {
        assert(item.hash > lastHash or (item.hash == lastHash and item.id >= lastDocId));

        const diffHash = item.hash - lastHash;
        const diffDocId = if (diffHash > 0) item.id else item.id - lastDocId;

        if (ptr + varint32Size(diffHash) + varint32Size(diffDocId) > data.len) {
            break;
        }

        ptr += writeVarint32(data[ptr..], diffHash);
        ptr += writeVarint32(data[ptr..], diffDocId);
        numItems += 1;

        lastHash = item.hash;
        lastDocId = item.id;
    }

    std.mem.writeInt(u16, data[0..2], numItems, .little);
    @memset(data[ptr..], 0);

    return numItems;
}

test "writeBlock/readBlock/readFirstItemFromBlock" {
    var items = std.ArrayList(Item).init(std.testing.allocator);
    defer items.deinit();

    try items.append(.{ .hash = 1, .id = 1 });
    try items.append(.{ .hash = 2, .id = 1 });
    try items.append(.{ .hash = 3, .id = 1 });
    try items.append(.{ .hash = 3, .id = 2 });
    try items.append(.{ .hash = 4, .id = 1 });

    const blockSize = 1024;
    var blockData: [blockSize]u8 = undefined;

    const numItems = try writeBlock(blockData[0..], items.items);
    try testing.expectEqual(items.items.len, numItems);

    items.clearRetainingCapacity();

    try readBlock(blockData[0..], &items);
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

    const item = try readFirstItemFromBlock(blockData[0..]);
    try testing.expectEqual(items.items[0], item);
}

const header_magic_v1 = 0x21f75da5;
const footer_magic_v1 = 0x5fb83a32;

pub const Header = packed struct {
    magic: u32 = header_magic_v1,
    version: u32,
    num_docs: u32,
    num_items: u32,
    block_size: u32,
};

pub const Footer = packed struct {
    magic: u32 = footer_magic_v1,
    num_blocks: u32,
};

const reserved_header_size = 256;
const header_size = @sizeOf(Header);

const reserved_footer_size = 64;
const footer_size = @sizeOf(Footer);

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

pub fn readFooter(reader: anytype) !Footer {
    const footer = try reader.readStructEndian(Footer, .little);
    try skipPadding(reader, reserved_footer_size, footer_size);
    return footer;
}

pub fn writeFooter(writer: anytype, footer: Footer) !void {
    assert(footer.magic == footer_magic_v1);
    try writer.writeStructEndian(footer, .little);
    try writePadding(writer, reserved_footer_size, footer_size);
}

const DocInfo = packed struct(u64) {
    id: u32,
    version: u24,
    deleted: u8,
};

pub fn writeFile(writer: anytype, segment: *InMemorySegment) !void {
    const block_size = default_block_size;

    const header = Header{
        .version = segment.version,
        .num_docs = @intCast(segment.docs.count()),
        .num_items = @intCast(segment.items.items.len),
        .block_size = block_size,
    };
    try writeHeader(writer, header);

    var docs_iter = segment.docs.iterator();
    while (docs_iter.next()) |entry| {
        const info = DocInfo{
            .id = entry.key_ptr.*,
            .version = 0,
            .deleted = if (entry.value_ptr.*) 0 else 1,
        };
        try writer.writeStructEndian(info, .little);
    }

    var num_blocks: usize = 0;
    var block_data: [block_size]u8 = undefined;
    var items = segment.items.items[0..];
    while (items.len > 0) {
        const n = try writeBlock(block_data[0..], items);
        items = items[n..];
        try writer.writeAll(block_data[0..]);
        num_blocks += 1;
    }

    const footer = Footer{ .num_blocks = @intCast(num_blocks) };
    try writeFooter(writer, footer);
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

    var fixed_buffer_steeam = std.io.fixedBufferStream(raw_data[0..]);
    var reader = fixed_buffer_steeam.reader();

    const header = try readHeader(reader);
    if (header.magic != header_magic_v1) {
        return error.InvalidSegment;
    }

    if (header.block_size < min_block_size or header.block_size > max_block_size) {
        return error.InvalidSegment;
    }

    const blocks_data_start = reserved_header_size + header.num_docs * @sizeOf(DocInfo);
    const blocks_data_size = file_size - reserved_footer_size - blocks_data_start;
    const blocks_data_end = blocks_data_start + blocks_data_size;
    if (blocks_data_size % header.block_size != 0) {
        return error.InvalidSegment;
    }
    const num_blocks = blocks_data_size / header.block_size;

    segment.version[0] = header.version;
    segment.version[1] = header.version;
    segment.block_size = header.block_size;

    try segment.docs.ensureTotalCapacity(header.num_docs);

    for (0..header.num_docs) |_| {
        const info = try reader.readStructEndian(DocInfo, .little);
        try segment.docs.put(info.id, info.deleted == 0);
    }

    try segment.index.ensureTotalCapacity(num_blocks);
    segment.blocks = raw_data[blocks_data_start..blocks_data_end];

    var i: usize = 0;
    while (i < num_blocks) : (i += 1) {
        const block_data = segment.getBlockData(i);
        const item = try readFirstItemFromBlock(block_data);
        try segment.index.append(item.hash);
    }

    try reader.skipBytes(blocks_data_size, .{});

    const footer = try readFooter(reader);
    if (footer.magic != footer_magic_v1) {
        return error.InvalidSegment;
    }
    if (footer.num_blocks != segment.index.items.len) {
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

        in_memory_segment.version = 1;
        try in_memory_segment.docs.put(1, true);
        try in_memory_segment.items.append(Item{ .hash = 1, .id = 1 });
        try in_memory_segment.items.append(Item{ .hash = 2, .id = 1 });

        in_memory_segment.ensureSorted();

        try writeFile(file.writer(), &in_memory_segment);
    }

    {
        var file = try tmp.dir.openFile("test.dat", .{});
        defer file.close();

        var segment = Segment.init(testing.allocator);
        defer segment.deinit();

        try readFile(file, &segment);

        try testing.expectEqual(1, segment.version[0]);
        try testing.expectEqual(1, segment.version[1]);
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

const index_header_magic_v1: u32 = 0x494e5844;

const IndexHeader = packed struct {
    magic: u32,
    version: u32,
    num_segments: u32,
};

pub fn writeIndexFile(writer: anytype, segments: std.ArrayList(Segment.Version)) !void {
    const header = IndexHeader{
        .magic = index_header_magic_v1,
        .version = 1,
        .num_segments = @intCast(segments.items.len),
    };
    try writer.writeStructEndian(header, .little);
    for (segments.items) |segment| {
        try writer.writeInt(u32, segment[0], .little);
        try writer.writeInt(u32, segment[1], .little);
    }
}

pub fn readIndexfile(reader: anytype, segments: *std.ArrayList(Segment.Version)) !void {
    const header = try reader.readStructEndian(IndexHeader, .little);
    if (header.magic != index_header_magic_v1) {
        return error.InvalidIndexfile;
    }
    if (header.version != 1) {
        return error.InvalidIndexfile;
    }
    try segments.ensureTotalCapacity(header.num_segments);
    for (0..header.num_segments) |_| {
        const v0 = try reader.readInt(u32, .little);
        const v1 = try reader.readInt(u32, .little);
        try segments.append(Segment.Version{ v0, v1 });
    }
}

test "readIndexfile/writeIndexfile" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var segments = std.ArrayList(Segment.Version).init(testing.allocator);
    defer segments.deinit();

    try segments.append(Segment.Version{ 1, 1 });
    try segments.append(Segment.Version{ 2, 3 });
    try segments.append(Segment.Version{ 4, 4 });

    {
        var file = try tmp.dir.createFile("test.idx", .{});
        defer file.close();

        try writeIndexFile(file.writer(), segments);
    }

    {
        var file = try tmp.dir.openFile("test.idx", .{});
        defer file.close();

        var segments2 = std.ArrayList(Segment.Version).init(testing.allocator);
        defer segments2.deinit();

        try readIndexfile(file.reader(), &segments2);

        try testing.expectEqualSlices(Segment.Version, segments.items, segments2.items);
    }
}
