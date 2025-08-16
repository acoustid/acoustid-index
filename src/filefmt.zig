// Segment file format layout:
// 1. Header - msgpack encoded segment metadata and configuration
// 2. Attributes - msgpack encoded string to u64 mappings
// 3. Documents - msgpack encoded document ID to boolean mappings
// 4. Padding - zero bytes to align to block size boundary
// 5. Blocks - fixed-size blocks containing the inverted index data
// 6. Block Index - uncompressed u32 array of max_hash for each block
// 7. Footer - msgpack encoded validation data (counts, checksum)
// 8. Footer Size - 4-byte little-endian u32 with footer size in bytes

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
const block = @import("block.zig");
const BlockReader = block.BlockReader;
const BlockEncoder = block.BlockEncoder;
const BlockHeader = block.BlockHeader;
const decodeBlockHeader = block.decodeBlockHeader;

pub const default_block_size = 512;
pub const min_block_size = block.MIN_BLOCK_SIZE;
pub const max_block_size = block.MAX_BLOCK_SIZE;

pub fn maxItemsPerBlock(block_size: usize) usize {
    return block_size / 2;
}

pub const max_file_name_size = 64;
const segment_file_name_fmt = "{x:0>16}-{x:0>8}.data";
pub const manifest_file_name = "manifest";

pub fn buildSegmentFileName(buf: []u8, info: SegmentInfo) []u8 {
    assert(buf.len == max_file_name_size);
    return std.fmt.bufPrint(buf, segment_file_name_fmt, .{ info.version, info.merges }) catch unreachable;
}

// Use block header from block.zig (already imported above)

pub const WriteBlocksResult = struct {
    footer: SegmentFileFooter,
    max_hashes: []u32,
};

pub fn writeBlocks(reader: anytype, writer: anytype, min_doc_id: u32, comptime block_size: u32, allocator: std.mem.Allocator) !WriteBlocksResult {
    var encoder = BlockEncoder.init();
    var items_buffer: [block.MAX_ITEMS_PER_BLOCK]Item = undefined;
    var items_in_buffer: usize = 0;
    var num_items: u32 = 0;
    var num_blocks: u32 = 0;
    var crc = std.hash.crc.Crc64Xz.init();
    var block_data: [block_size]u8 = undefined;
    var max_hashes = std.ArrayList(u32).init(allocator);

    while (true) {
        // Fill buffer with new items from reader
        while (items_in_buffer < items_buffer.len) {
            const item = try reader.read() orelse break;
            items_buffer[items_in_buffer] = item;
            items_in_buffer += 1;
            reader.advance();
        }

        // Encode a block from the buffer
        const items_consumed = try encoder.encodeBlock(items_buffer[0..items_in_buffer], min_doc_id, &block_data);
        try writer.writeAll(&block_data);
        if (items_consumed == 0) {
            break;
        }

        // Calculate max_hash from the consumed items
        const max_hash = items_buffer[items_consumed - 1].hash;
        try max_hashes.append(max_hash);

        num_items += @intCast(items_consumed);
        num_blocks += 1;
        crc.update(&block_data);

        // Move unused items to front of buffer
        const remaining = items_in_buffer - items_consumed;
        if (remaining > 0) {
            std.mem.copyForwards(Item, items_buffer[0..remaining], items_buffer[items_consumed..items_in_buffer]);
        }
        items_in_buffer = remaining;
    }

    return WriteBlocksResult{
        .footer = SegmentFileFooter{
            .magic = segment_file_footer_magic_v1,
            .num_items = num_items,
            .num_blocks = num_blocks,
            .checksum = crc.final(),
        },
        .max_hashes = try max_hashes.toOwnedSlice(),
    };
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

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();
    
    const blocks_result = try writeBlocks(reader, writer, segment.min_doc_id, block_size, arena_allocator);
    defer arena_allocator.free(blocks_result.max_hashes);

    // Write block index (uncompressed u32 array of max_hash for each block)
    for (blocks_result.max_hashes) |max_hash| {
        try writer.writeInt(u32, max_hash, .little);
    }

    // Write footer and capture its size
    const footer_start_pos = counting_writer.bytes_written;
    try packer.write(blocks_result.footer);
    const footer_size = counting_writer.bytes_written - footer_start_pos;

    // Write footer size as 4-byte little-endian integer at the end
    try writer.writeInt(u32, @intCast(footer_size), .little);

    try buffered_writer.flush();

    try file.file.sync();

    try file.finish();

    log.info("wrote segment file {s} (blocks = {}, items = {}, checksum = {})", .{
        file_name,
        blocks_result.footer.num_blocks,
        blocks_result.footer.num_items,
        blocks_result.footer.checksum,
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

    segment.attributes.clearRetainingCapacity();
    if (header.has_attributes) {
        try msgpack.unpackMapInto(reader, segment.allocator, &segment.attributes);
    }

    segment.min_doc_id = 0;
    segment.max_doc_id = 0;

    segment.docs.clearRetainingCapacity();

    if (header.has_docs) {
        try msgpack.unpackMapInto(reader, segment.allocator, &segment.docs);

        var iter = segment.docs.keyIterator();
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

    // No need to allocate capacity since we'll use a direct slice

    var num_items: u32 = 0;
    var num_blocks: u32 = 0;
    var crc = std.hash.crc.Crc64Xz.init();

    var ptr = blocks_data_start;
    while (ptr + block_size <= raw_data.len) {
        const block_data = raw_data[ptr .. ptr + block_size];
        ptr += block_size;
        const block_header = decodeBlockHeader(block_data);
        if (block_header.num_items == 0) {
            break;
        }
        num_items += block_header.num_items;
        num_blocks += 1;
        crc.update(block_data);
    }
    const blocks_data_end = ptr;
    // The empty block (included in blocks_data_start..blocks_data_end) provides sufficient SIMD padding
    segment.blocks = raw_data[blocks_data_start..blocks_data_end];
    segment.num_blocks = num_blocks;

    try fixed_buffer_stream.seekBy(@intCast(segment.blocks.len));

    // Read block index (uncompressed u32 array of max_hash for each block)
    // Use the mmap-ed memory directly instead of allocating
    const block_index_start = fixed_buffer_stream.pos;
    const block_index_size = num_blocks * @sizeOf(u32);
    const block_index_end = block_index_start + block_index_size;
    
    if (block_index_end > raw_data.len) {
        return error.InvalidSegment;
    }
    
    // Cast the mmap-ed memory to a u32 slice (assuming little-endian)
    const block_index_bytes = raw_data[block_index_start..block_index_end];
    segment.block_index = @as([*]const u32, @ptrCast(@alignCast(block_index_bytes.ptr)))[0..num_blocks];
    
    try fixed_buffer_stream.seekBy(@intCast(block_index_size));

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
        try testing.expectEqual(1, segment.block_index.len);
        try testing.expectEqual(2, segment.block_index[0]); // max_hash of the block

        var block_reader = BlockReader.init(segment.min_doc_id);
        segment.loadBlockData(0, &block_reader, false);

        try std.testing.expectEqualSlices(u32, &[_]u32{ 1, 2 }, block_reader.getHashes());
        try std.testing.expectEqualSlices(u32, &[_]u32{ 1, 1 }, block_reader.getDocids());
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
