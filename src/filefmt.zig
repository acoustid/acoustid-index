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

pub fn readFirstItemFromBlock(data: []const u8) !Item {
    if (data.len < 2 + min_varint32_size * 2) {
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

pub fn writeBlock(data: []u8, items: []const Item) !usize {
    assert(data.len >= 2);

    var ptr: usize = 2;
    var num_items: u16 = 0;
    var last_hash: u32 = 0;
    var last_doc_id: u32 = 0;

    for (items) |item| {
        assert(item.hash > last_hash or (item.hash == last_hash and item.id >= last_doc_id));

        const diff_hash = item.hash - last_hash;
        const diff_doc_id = if (diff_hash > 0) item.id else item.id - last_doc_id;

        if (ptr + varint32Size(diff_hash) + varint32Size(diff_doc_id) > data.len) {
            break;
        }

        ptr += writeVarint32(data[ptr..], diff_hash);
        ptr += writeVarint32(data[ptr..], diff_doc_id);
        num_items += 1;

        last_hash = item.hash;
        last_doc_id = item.id;
    }

    std.mem.writeInt(u16, data[0..2], num_items, .little);
    @memset(data[ptr..], 0);

    return num_items;
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
    version: u32,
    included_merges: u32,
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
        .version = segment.id.version,
        .included_merges = segment.id.included_merges,
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

const SegmentIter = struct {
    reader: Segment.Reader,
    has_more: bool = true,
    item: ?Item = null,
    skip_docs: std.AutoHashMap(u32, void),

    pub fn init(allocator: std.mem.Allocator, segment: *Segment) SegmentIter {
        return .{
            .reader = segment.reader(),
            .skip_docs = std.AutoHashMap(u32, void).init(allocator),
        };
    }

    pub fn deinit(self: *SegmentIter) void {
        self.reader.close();
        self.skip_docs.deinit();
    }

    pub fn load(self: *SegmentIter) !void {
        while (self.item == null and self.has_more) {
            try self.reader.load();
            if (self.reader.item) |item| {
                if (self.skip_docs.contains(item.id)) {
                    self.reader.item = null;
                    continue;
                } else {
                    self.reader.item = null;
                    self.item = item;
                    return;
                }
            } else {
                self.has_more = false;
                return;
            }
        }
    }

    pub fn advance(self: *SegmentIter) void {
        self.item = null;
    }
};

fn getNextItem(sources: *[2]SegmentIter) !?Item {
    inline for (sources) |*source| {
        try source.load();
    }

    var next_item: ?Item = null;
    inline for (sources) |*source| {
        if (source.item) |item| {
            if (next_item == null or Item.cmp({}, item, next_item.?)) {
                next_item = item;
                source.advance();
            }
        }
    }
    return next_item;
}

pub fn mergeAndWriteFile(file: fs.File, segments_to_merge: Segment.List.SegmentsToMerge, collection: Segment.List, allocator: std.mem.Allocator) !void {
    const segment1 = &segments_to_merge.node1.data;
    const segment2 = &segments_to_merge.node2.data;

    const block_size = default_block_size;

    const writer = file.writer();

    var header = Header{
        .version = @min(segment1.id.version, segment2.id.version),
        .included_merges = 1 + segment1.id.included_merges + segment2.id.included_merges,
        .num_docs = 0,
        .num_items = 0,
        .num_blocks = 0,
        .block_size = block_size,
        .max_commit_id = @max(segment1.max_commit_id, segment2.max_commit_id),
    };
    try writeHeader(writer, header);

    var sources: [2]SegmentIter = undefined;

    sources[0] = SegmentIter.init(allocator, segment1);
    defer sources[0].deinit();

    sources[1] = SegmentIter.init(allocator, segment2);
    defer sources[1].deinit();

    for (&sources) |*source| {
        var docs_iter = source.reader.segment.docs.iterator();
        while (docs_iter.next()) |entry| {
            const id = entry.key_ptr.*;
            const status = entry.value_ptr.*;
            if (!collection.hasNewerVersion(id, source.reader.segment.id.version)) {
                const info = DocInfo{
                    .id = id,
                    .version = @intCast(source.reader.segment.id.version - header.version),
                    .deleted = if (status) 0 else 1,
                };
                try writer.writeStructEndian(info, .little);
                header.num_docs += 1;
            } else {
                try source.skip_docs.put(id, {});
            }
        }
    }

    var block_data: [block_size]u8 = undefined;

    var items = std.ArrayList(Item).init(allocator);
    defer items.deinit();

    const max_items_per_block = maxItemsPerBlock(block_size);
    while (true) {
        // fill up the buffer wirh items from both segments
        while (items.items.len < max_items_per_block) {
            const item = try getNextItem(&sources);
            if (item) |i| {
                try items.append(i);
            } else {
                break;
            }
        }
        if (items.items.len == 0) {
            break;
        }

        // write the buffer to the file
        const n = try writeBlock(block_data[0..], items.items[0..]);
        try file.writeAll(block_data[0..]);
        items.items = items.items[n..];
        header.num_items += @intCast(n);
        header.num_blocks += 1;
    }

    try file.seekTo(0);
    try writeHeader(writer, header);
}

test "mergeAndWriteFile" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var data_dir = try tmp.dir.makeOpenPath("data", .{});
    defer data_dir.close();

    var segments = Segment.List.init(std.testing.allocator);
    defer segments.deinit();

    var in_memory_segment_1 = InMemorySegment.init(std.testing.allocator);
    defer in_memory_segment_1.deinit();

    in_memory_segment_1.id.version = 1;
    in_memory_segment_1.frozen = true;

    var segment_1 = try segments.createSegment();
    try segment_1.data.convert(data_dir, &in_memory_segment_1);
    segments.segments.append(segment_1);

    var in_memory_segment_2 = InMemorySegment.init(std.testing.allocator);
    defer in_memory_segment_2.deinit();

    in_memory_segment_2.id.version = 2;
    in_memory_segment_2.frozen = true;

    var segment_2 = try segments.createSegment();
    try segment_2.data.convert(data_dir, &in_memory_segment_2);
    segments.segments.append(segment_2);

    var segment_3 = Segment.init(std.testing.allocator);
    defer segment_3.deinit();

    try segment_3.merge(data_dir, .{ .node1 = segment_1, .node2 = segment_2 }, segments);
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
    const blocks_data_size = file_size - blocks_data_start;
    const blocks_data_end = blocks_data_start + blocks_data_size;
    if (blocks_data_size % header.block_size != 0) {
        return error.InvalidSegment;
    }
    const num_blocks = blocks_data_size / header.block_size;
    if (num_blocks != header.num_blocks) {
        return error.InvalidSegment;
    }

    segment.id.version = header.version;
    segment.id.included_merges = header.included_merges;
    segment.block_size = header.block_size;
    segment.max_commit_id = header.max_commit_id;
    segment.num_items = header.num_items;

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

        in_memory_segment.id.version = 1;
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

// "IDX1"
const index_header_magic_v1: u32 = 0x49445831;

const IndexHeader = packed struct {
    magic: u32,
    version: u32,
    num_segments: u32,
};

pub fn writeIndexFile(writer: anytype, segments: std.ArrayList(SegmentVersion)) !void {
    const header = IndexHeader{
        .magic = index_header_magic_v1,
        .version = 1,
        .num_segments = @intCast(segments.items.len),
    };
    try writer.writeStructEndian(header, .little);
    for (segments.items) |segment| {
        try writer.writeInt(u32, segment.version, .little);
        try writer.writeInt(u32, segment.included_merges, .little);
    }
}

pub fn readIndexFile(reader: anytype, segments: *std.ArrayList(SegmentVersion)) !void {
    const header = try reader.readStructEndian(IndexHeader, .little);
    if (header.magic != index_header_magic_v1) {
        return error.InvalidIndexfile;
    }
    if (header.version != 1) {
        return error.InvalidIndexfile;
    }
    try segments.ensureTotalCapacity(header.num_segments);
    for (0..header.num_segments) |_| {
        var version: SegmentVersion = undefined;
        version.version = try reader.readInt(u32, .little);
        version.included_merges = try reader.readInt(u32, .little);
        try segments.append(version);
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
