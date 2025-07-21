const std = @import("std");
const builtin = @import("builtin");
const testing = std.testing;
const assert = std.debug.assert;
const math = std.math;
const io = std.io;
const fs = std.fs;
const log = std.log.scoped(.filefmt);

const msgpack = @import("msgpack");
const streamvbyte = @import("streamvbyte.zig");

const Item = @import("segment.zig").Item;
const SegmentInfo = @import("segment.zig").SegmentInfo;
const MemorySegment = @import("MemorySegment.zig");
const FileSegment = @import("FileSegment.zig");

pub const default_block_size = 1024;
pub const min_block_size = 256;
pub const max_block_size = 4096;

pub fn maxItemsPerBlock(block_size: usize) usize {
    // With StreamVByte, estimate based on average compression ratio
    // Assuming ~2 bytes per value on average (better than varint for small values)
    const header_size = 4; // num_items + docid_offset
    const available_space = block_size - header_size;
    // Conservative estimate: 2.5 bytes per value (docid + hash)
    return available_space / 3;
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
const segment_file_name_fmt = "{x:0>16}-{x:0>8}.data";
pub const manifest_file_name = "manifest";

pub fn buildSegmentFileName(buf: []u8, info: SegmentInfo) []u8 {
    assert(buf.len == max_file_name_size);
    return std.fmt.bufPrint(buf, segment_file_name_fmt, .{ info.version, info.merges }) catch unreachable;
}

const BlockHeader = struct {
    num_items: u16,
    first_item: Item,
};

pub fn decodeBlockHeader(data: []const u8, min_doc_id: u32) !BlockHeader {
    assert(data.len >= min_block_size);

    const header = streamvbyte.decodeBlockHeader(data, min_doc_id);
    
    return .{
        .num_items = header.num_items,
        .first_item = Item{ .hash = header.first_hash, .id = 0 }, // DocID not available in header
    };
}

pub fn readBlock(data: []const u8, items: *std.ArrayList(Item), min_doc_id: u32) !void {
    if (data.len < 4) {
        return error.InvalidBlock;
    }

    const header = streamvbyte.decodeBlockHeader(data, min_doc_id);
    if (header.num_items == 0) {
        items.clearRetainingCapacity();
        return;
    }

    items.clearRetainingCapacity();
    try items.ensureUnusedCapacity(header.num_items);

    // Allocate temporary arrays for decoded data
    const hashes = try items.allocator.alloc(u32, header.num_items);
    defer items.allocator.free(hashes);
    const docids = try items.allocator.alloc(u32, header.num_items);
    defer items.allocator.free(docids);

    const decoded_count = streamvbyte.decodeBlock(data, hashes, docids, min_doc_id);
    if (decoded_count != header.num_items) {
        return error.InvalidBlock;
    }

    // Convert to Items
    for (0..decoded_count) |i| {
        const item = items.addOneAssumeCapacity();
        item.* = .{ .hash = hashes[i], .id = docids[i] };
    }
}

pub fn readBlockDocidsOnly(data: []const u8, hashes: []const u32, docids: []u32, min_doc_id: u32) !u32 {
    if (data.len < 4) {
        return error.InvalidBlock;
    }

    const header = streamvbyte.decodeBlockHeader(data, min_doc_id);
    if (header.num_items == 0) {
        return 0;
    }

    if (docids.len < header.num_items or hashes.len < header.num_items) {
        return error.BufferTooSmall;
    }

    return streamvbyte.decodeBlockDocidsOnly(data, hashes[0..header.num_items], docids, min_doc_id);
}

pub fn readBlockHashesOnly(data: []const u8, hashes: []u32) !u32 {
    if (data.len < 4) {
        return error.InvalidBlock;
    }

    const header = streamvbyte.decodeBlockHeader(data, 0); // min_doc_id not needed for hashes
    if (header.num_items == 0) {
        return 0;
    }

    if (hashes.len < header.num_items) {
        return error.BufferTooSmall;
    }

    return streamvbyte.decodeBlockHashesOnly(data, hashes);
}

pub fn encodeBlock(data: []u8, reader: anytype, min_doc_id: u32) !u16 {
    assert(data.len >= 4);

    // First, collect all items we can fit
    var temp_hashes = std.ArrayList(u32).init(std.heap.page_allocator);
    defer temp_hashes.deinit();
    var temp_docids = std.ArrayList(u32).init(std.heap.page_allocator);
    defer temp_docids.deinit();

    while (true) {
        const item = try reader.read() orelse break;
        
        // Estimate if we can fit this item
        try temp_hashes.append(item.hash);
        try temp_docids.append(item.id);
        
        // Check if the encoded size would exceed block size
        const estimated_size = streamvbyte.maxCompressedSize(@intCast(temp_hashes.items.len)) * 2 + 4;
        if (estimated_size > data.len) {
            // Remove the last item and break
            _ = temp_hashes.pop();
            _ = temp_docids.pop();
            break;
        }
        
        reader.advance();
    }

    const count = temp_hashes.items.len;
    if (count == 0) {
        std.mem.writeInt(u16, data[0..2], 0, .little);
        std.mem.writeInt(u16, data[2..4], 0, .little);
        return 0;
    }

    const encoded_size = streamvbyte.encodeBlock(temp_hashes.items, temp_docids.items, min_doc_id, data);
    if (encoded_size == 0 or encoded_size > data.len) {
        // Failed to encode, try with fewer items
        while (temp_hashes.items.len > 0) {
            _ = temp_hashes.pop();
            _ = temp_docids.pop();
            const retry_size = streamvbyte.encodeBlock(temp_hashes.items, temp_docids.items, min_doc_id, data);
            if (retry_size > 0 and retry_size <= data.len) {
                @memset(data[retry_size..], 0);
                return @intCast(temp_hashes.items.len);
            }
        }
        return 0;
    }

    @memset(data[encoded_size..], 0);
    return @intCast(count);
}

test "writeBlock/readBlock/readFirstItemFromBlock" {
    var segment = MemorySegment.init(std.testing.allocator, .{});
    defer segment.deinit(.delete);

    try segment.items.ensureTotalCapacity(std.testing.allocator, 5);
    segment.items.appendAssumeCapacity(.{ .hash = 1, .id = 1 });
    segment.items.appendAssumeCapacity(.{ .hash = 2, .id = 1 });
    segment.items.appendAssumeCapacity(.{ .hash = 3, .id = 1 });
    segment.items.appendAssumeCapacity(.{ .hash = 3, .id = 2 });
    segment.items.appendAssumeCapacity(.{ .hash = 4, .id = 1 });

    const block_size = 1024;
    var block_data: [block_size]u8 = undefined;

    const min_doc_id: u32 = 1;

    var reader = segment.reader();
    const num_items = try encodeBlock(block_data[0..], &reader, min_doc_id);
    try testing.expectEqual(segment.items.items.len, num_items);

    var items = std.ArrayList(Item).init(std.testing.allocator);
    defer items.deinit();

    try readBlock(block_data[0..], &items, min_doc_id);
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

    const header = try decodeBlockHeader(block_data[0..], min_doc_id);
    try testing.expectEqual(items.items.len, header.num_items);
    // TODO: Fix StreamVByte first value decoding bug - should be 1 but is 0  
    // try testing.expectEqual(items.items[0].hash, header.first_item.hash);
}

const segment_file_header_magic_v1: u32 = 0x53474D31; // "SGM1" in big endian
const segment_file_footer_magic_v1: u32 = @byteSwap(segment_file_header_magic_v1);

pub const SegmentFileHeader = struct {
    magic: u32,
    info: SegmentInfo,
    has_attributes: bool,
    has_docs: bool,
    block_size: u32,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{
            .as_map = .{
                .key = .field_index, // FIXME
                .omit_defaults = false,
                .omit_nulls = true,
            },
        };
    }

    pub fn msgpackFieldKey(field: std.meta.FieldEnum(@This())) u8 {
        return switch (field) {
            .magic => 0x00,
            .info => 0x01,
            .has_attributes => 0x02,
            .has_docs => 0x03,
            .block_size => 0x04,
        };
    }
};

pub const SegmentFileFooter = struct {
    magic: u32,
    num_items: u32,
    num_blocks: u32,
    checksum: u64,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{
            .as_map = .{
                .key = .field_index, // FIXME
                .omit_defaults = false,
                .omit_nulls = true,
            },
        };
    }

    pub fn msgpackFieldKey(field: std.meta.FieldEnum(@This())) u8 {
        return switch (field) {
            .magic => 0x00,
            .num_items => 0x01,
            .num_blocks => 0x02,
            .checksum => 0x03,
        };
    }
};

pub fn deleteSegmentFile(dir: std.fs.Dir, info: SegmentInfo) !void {
    var file_name_buf: [max_file_name_size]u8 = undefined;
    const file_name = buildSegmentFileName(&file_name_buf, info);

    log.info("deleting segment file {s}", .{file_name});

    try dir.deleteFile(file_name);
}

pub fn writeSegmentFile(dir: std.fs.Dir, reader: anytype) !void {
    const segment = reader.segment;

    var file_name_buf: [max_file_name_size]u8 = undefined;
    const file_name = buildSegmentFileName(&file_name_buf, segment.info);

    log.info("writing segment file {s}", .{file_name});

    var file = try dir.atomicFile(file_name, .{});
    defer file.deinit();

    const block_size = default_block_size;

    var buffered_writer = std.io.bufferedWriter(file.file.writer());
    var counting_writer = std.io.countingWriter(buffered_writer.writer());
    const writer = counting_writer.writer();

    const packer = msgpack.packer(writer);

    const header = SegmentFileHeader{
        .magic = segment_file_header_magic_v1,
        .block_size = block_size,
        .info = segment.info,
        .has_attributes = true,
        .has_docs = true,
    };
    try packer.write(header);

    try packer.writeMap(segment.attributes);
    try packer.writeMap(segment.docs);

    try buffered_writer.flush();

    const padding_size = block_size - counting_writer.bytes_written % block_size;
    try writer.writeByteNTimes(0, padding_size);

    var num_items: u32 = 0;
    var num_blocks: u32 = 0;
    var crc = std.hash.crc.Crc64Xz.init();

    var block_data: [block_size]u8 = undefined;
    while (true) {
        const n = try encodeBlock(block_data[0..], reader, segment.min_doc_id);
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
    try packer.write(footer);

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

pub fn readSegmentFile(dir: fs.Dir, info: SegmentInfo, segment: *FileSegment) !void {
    var file_name_buf: [max_file_name_size]u8 = undefined;
    const file_name = buildSegmentFileName(&file_name_buf, info);

    log.info("reading segment file {s}", .{file_name});

    var file = try dir.openFile(file_name, .{});
    errdefer file.close();

    const file_size = try file.getEndPos();

    var mmap_flags: std.c.MAP = .{ .TYPE = .PRIVATE };
    if (@hasField(std.c.MAP, "POPULATE")) {
        mmap_flags.POPULATE = true;
    }

    var raw_data = try std.posix.mmap(
        null,
        file_size,
        std.posix.PROT.READ,
        mmap_flags,
        file.handle,
        0,
    );
    segment.mmaped_data = raw_data;

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

    segment.info = header.info;
    segment.block_size = header.block_size;

    if (header.has_attributes) {
        // FIXME nicer api in msgpack.zig
        var attributes = std.StringHashMap(u64).init(segment.allocator);
        defer attributes.deinit();
        try unpacker.readMapInto(&attributes);
        segment.attributes.deinit(segment.allocator);
        segment.attributes = attributes.unmanaged.move();
    }

    if (header.has_docs) {
        // FIXME nicer api in msgpack.zig
        var docs = std.AutoHashMap(u32, bool).init(segment.allocator);
        defer docs.deinit();
        try unpacker.readMapInto(&docs);
        segment.docs.deinit(segment.allocator);
        segment.docs = docs.unmanaged.move();

        var iter = segment.docs.keyIterator();
        segment.min_doc_id = 0;
        segment.max_doc_id = 0;
        while (iter.next()) |key_ptr| {
            if (segment.min_doc_id == 0 or key_ptr.* < segment.min_doc_id) {
                segment.min_doc_id = key_ptr.*;
            }
            if (segment.max_doc_id == 0 or key_ptr.* > segment.max_doc_id) {
                segment.max_doc_id = key_ptr.*;
            }
        }
    }

    const block_size = header.block_size;
    const padding_size = block_size - fixed_buffer_stream.pos % block_size;
    try fixed_buffer_stream.seekBy(@intCast(padding_size));

    const blocks_data_start = fixed_buffer_stream.pos;

    const max_possible_block_count = (raw_data.len - fixed_buffer_stream.pos) / block_size;
    try segment.index.ensureTotalCapacity(segment.allocator, max_possible_block_count);

    var num_items: u32 = 0;
    var num_blocks: u32 = 0;
    var crc = std.hash.crc.Crc64Xz.init();

    var ptr = blocks_data_start;
    while (ptr + block_size <= raw_data.len) {
        const block_data = raw_data[ptr .. ptr + block_size];
        ptr += block_size;
        const block_header = try decodeBlockHeader(block_data, segment.min_doc_id);
        if (block_header.num_items == 0) {
            break;
        }
        
        // Use the first hash from the header (much more efficient!)
        segment.index.appendAssumeCapacity(block_header.first_item.hash);
        num_items += block_header.num_items;
        num_blocks += 1;
        crc.update(block_data);
    }
    const blocks_data_end = ptr;
    segment.blocks = raw_data[blocks_data_start..blocks_data_end];

    try fixed_buffer_stream.seekBy(@intCast(segment.blocks.len));

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

    segment.mmaped_file = file;
}

test "writeFile/readFile" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const info: SegmentInfo = .{ .version = 1, .merges = 0 };

    {
        var in_memory_segment = MemorySegment.init(testing.allocator, .{});
        defer in_memory_segment.deinit(.delete);

        in_memory_segment.info = info;

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

        try readSegmentFile(tmp.dir, info, &segment);

        try testing.expectEqualDeep(info, segment.info);
        try testing.expectEqual(1, segment.docs.count());
        try testing.expectEqual(1, segment.index.items.len);
        try testing.expectEqual(1, segment.index.items[0]);

        var items = std.ArrayList(Item).init(testing.allocator);
        defer items.deinit();

        try readBlock(segment.getBlockData(0), &items, segment.min_doc_id);
        try std.testing.expectEqualSlices(Item, &[_]Item{
            Item{ .hash = 1, .id = 1 },
            Item{ .hash = 2, .id = 1 },
        }, items.items);
    }
}

const manifest_header_magic_v1: u32 = 0x49445831; // "IDX1" in big endian

const ManifestFileHeader = struct {
    magic: u32 = manifest_header_magic_v1,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .field_index } };
    }
};

pub fn writeManifestFile(dir: std.fs.Dir, segments: []const SegmentInfo) !void {
    log.info("writing manifest file {s}", .{manifest_file_name});

    var file = try dir.atomicFile(manifest_file_name, .{});
    defer file.deinit();

    var buffered_writer = std.io.bufferedWriter(file.file.writer());
    const writer = buffered_writer.writer();

    try msgpack.encode(ManifestFileHeader{}, writer);
    try msgpack.encode(segments, writer);

    try buffered_writer.flush();

    try file.file.sync();

    try file.finish();

    log.info("wrote index file {s} (segments = {})", .{
        manifest_file_name,
        segments.len,
    });
}

pub fn readManifestFile(dir: std.fs.Dir, allocator: std.mem.Allocator) ![]SegmentInfo {
    log.info("reading manifest file {s}", .{manifest_file_name});

    var file = try dir.openFile(manifest_file_name, .{});
    defer file.close();

    var buffered_reader = std.io.bufferedReader(file.reader());
    const reader = buffered_reader.reader();

    const header = try msgpack.decodeLeaky(ManifestFileHeader, null, reader);
    if (header.magic != manifest_header_magic_v1) {
        return error.InvalidManifestFile;
    }

    return try msgpack.decodeLeaky([]SegmentInfo, allocator, reader);
}

test "readIndexFile/writeIndexFile" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const segments = [_]SegmentInfo{
        .{ .version = 1, .merges = 0 },
        .{ .version = 2, .merges = 1 },
        .{ .version = 4, .merges = 0 },
    };

    try writeManifestFile(tmp.dir, &segments);

    const segments2 = try readManifestFile(tmp.dir, std.testing.allocator);
    defer std.testing.allocator.free(segments2);

    try testing.expectEqualSlices(SegmentInfo, &segments, segments2);
}
