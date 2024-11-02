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
        if (b & 0x80 == 0) {
            return .{ .value = v, .size = i + 1 };
        }
        shift += 7;
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

// "SGM1"
const header_magic_v1 = 0x53474d31;

// "1SGM"
const footer_magic_v1 = 0x3153474d;

pub const Header = packed struct {
    magic: u32 = header_magic_v1,
    version_low: u32,
    version_high: u32,
    num_docs: u32,
    num_items: u32,
    num_blocks: u32,
    block_size: u32,
    max_commit_id: u64,
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

pub fn writeFile(file: std.fs.File, segment: *InMemorySegment) !void {
    const block_size = default_block_size;

    const writer = file.writer();

    var header = Header{
        .version_low = segment.version,
        .version_high = segment.version,
        .num_docs = @intCast(segment.docs.count()),
        .num_items = @intCast(segment.items.items.len),
        .num_blocks = 0,
        .block_size = block_size,
        .max_commit_id = segment.max_commit_id,
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

    var block_data: [block_size]u8 = undefined;
    var items = segment.items.items[0..];
    while (items.len > 0) {
        const n = try writeBlock(block_data[0..], items);
        items = items[n..];
        try writer.writeAll(block_data[0..]);
        header.num_blocks += 1;
    }

    try file.seekTo(0);
    try writeHeader(writer, header);
}

const max_items_per_block = default_block_size;

const SegmentIter = struct {
    segment: *Segment,
    next_block_no: usize,
    items: std.ArrayList(Item),
    ptr: []Item,

    pub fn init(allocator: std.mem.Allocator, segment: *Segment) SegmentIter {
        return .{
            .segment = segment,
            .next_block_no = 0,
            .items = std.ArrayList(Item).init(allocator),
            .ptr = undefined,
        };
    }

    pub fn deinit(self: *SegmentIter) void {
        self.items.deinit();
    }

    pub fn loadNextBlockIfNeeded(self: *SegmentIter) !void {
        if (self.ptr.len == 0 and self.next_block_no < self.segment.index.len) {
            const block_data = try self.segment.readBlock(self.next_block_no);
            readBlock(block_data[0..], &self.items);
            self.ptr = self.items.items[0..];
            self.next_block_no += 1;
        }
    }
};

fn getNextItem(sources: *[2]SegmentIter) !?Item {
    if (sources[0].ptr.len > 0 and sources[1].ptr.len > 0) {
        const a = sources[0].ptr[0];
        const b = sources[1].ptr[0];
        if (a.id < b.id) {
            sources[0].ptr = sources[0].ptr[1..];
            return a;
        } else {
            sources[1].ptr = sources[1].ptr[1..];
            return b;
        }
    }

    for (sources) |*source| {
        try source.loadNextBlockIfNeeded();
    }

    var item: ?Item = null;

    if (sources[0].ptr.len > 0 and sources[1].ptr.len > 0) {
        const a = sources[0].ptr[0];
        const b = sources[1].ptr[0];
        if (a.id < b.id) {
            item = a;
            sources[0].ptr = sources[0].ptr[1..];
        } else {
            item = b;
            sources[1].ptr = sources[1].ptr[1..];
        }
    } else if (sources[0].ptr.len > 0) {
        item = sources[0].ptr[0];
        sources[0].ptr = sources[0].ptr[1..];
    } else if (sources[1].ptr.len > 0) {
        item = sources[1].ptr[0];
        sources[1].ptr = sources[1].ptr[1..];
    }

    return item;
}

pub fn writeFileFromTwoSegments(file: fs.File, segments: [2]*Segment, allocator: std.mem.Allocator) !void {
    const block_size = default_block_size;

    const writer = file.writer();

    const header = Header{
        .version_low = @min(segments[0].version[0], segments[1].version[0]),
        .version_high = @max(segments[0].version[1], segments[1].version[1]),
        .num_docs = 0,
        .num_items = 0,
        .num_blocks = 0,
        .block_size = block_size,
        .max_commit_id = @max(segments[0].max_commit_id, segments[1].max_commit_id),
    };
    try writeHeader(writer, header);

    // TODO filter out docs/items that are deleted in more recent segments

    for (segments, 0..) |*segment, i| {
        var docs_iter = segment.docs.iterator();
        while (docs_iter.next()) |entry| {
            const info = DocInfo{
                .id = entry.key_ptr.*,
                .version = i,
                .deleted = if (entry.value_ptr.*) 0 else 1,
            };
            try writer.writeStructEndian(info, .little);
            header.num_docs += 1;
        }
    }

    var items = std.ArrayList(Item).init(allocator);
    defer items.deinit();

    const sources: [2]SegmentIter = undefined;

    sources[0] = SegmentIter.init(allocator, segments[0]);
    defer sources[0].deinit();

    sources[1] = SegmentIter.init(allocator, segments[1]);
    defer sources[1].deinit();

    var block_data: [block_size]u8 = undefined;

    while (true) {
        // fill up the buffer wirh items from both segments
        while (items.len < max_items_per_block.len) {
            const item = try getNextItem(&sources);
            if (item == null) {
                break;
            }
            try items.append(item);
        }
        if (items.len == 0) {
            break;
        }

        // write the buffer to the file
        const n = try writeBlock(block_data[0..], items.items[0..]);
        try file.writeAll(block_data[0..]);
        items.items = items.items[n..];
        header.num_items += n;
        header.num_blocks += 1;
    }

    file.seekTo(0);
    try writeHeader(writer, header);
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
    if (header.num_blocks == 0 or header.num_docs == 0 or header.num_items == 0) {
        return error.InvalidSegment;
    }
    if (header.block_size < min_block_size or header.block_size > max_block_size) {
        return error.InvalidSegment;
    }

    const blocks_data_start = reserved_header_size + header.num_docs * @sizeOf(DocInfo);
    const blocks_data_size = file_size - blocks_data_start;
    const blocks_data_end = blocks_data_start + blocks_data_size;
    if (blocks_data_size % header.block_size != 0) {
        return error.InvalidSegment;
    }
    const num_blocks = blocks_data_size / header.block_size;
    if (num_blocks != header.num_blocks) {
        return error.InvalidSegment;
    }

    segment.version[0] = header.version_low;
    segment.version[1] = header.version_high;
    segment.block_size = header.block_size;
    segment.max_commit_id = header.max_commit_id;

    try segment.docs.ensureTotalCapacity(header.num_docs);

    for (0..header.num_docs) |_| {
        const info = try reader.readStructEndian(DocInfo, .little);
        try segment.docs.put(info.id, info.deleted == 0);
    }

    try segment.index.ensureTotalCapacity(num_blocks);
    segment.blocks = raw_data[blocks_data_start..blocks_data_end];

    var i: u32 = 0;
    while (i < header.num_blocks) : (i += 1) {
        const block_data = segment.getBlockData(i);
        const item = try readFirstItemFromBlock(block_data);
        try segment.index.append(item.hash);
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

        try writeFile(file, &in_memory_segment);
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

// "IDX1"
const index_header_magic_v1: u32 = 0x49445831;

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

pub fn readIndexFile(reader: anytype, segments: *std.ArrayList(Segment.Version)) !void {
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

test "readIndexFile/writeIndexFile" {
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

        try readIndexFile(file.reader(), &segments2);

        try testing.expectEqualSlices(Segment.Version, segments.items, segments2.items);
    }
}
