const std = @import("std");
const Item = @import("segment.zig").Item;

const c = @cImport({
    @cInclude("streamvbyte_decode.h");
});

// Encode lists of (hash, docid) pairs into blocks, encoded with StreamVByte.
// Each block has a fixed size and are written into a file, they need to be fixed size for easier indexing.

// Block format:
//  - u16   num items
//  - u16   docid list offset
//  - u32   first hash
//  - []u8  encoded hash list
//  - []u8  padding
//  - []u8  encoded docid list

pub const MIN_BLOCK_SIZE = 64;
pub const MAX_BLOCK_SIZE = 4096;
pub const MAX_ITEMS_PER_BLOCK = MAX_BLOCK_SIZE / 2;

pub const BLOCK_HEADER_SIZE = 8; // u16 + u16 + u32

// Padding required for SIMD decode functions to safely read 16 bytes
pub const SIMD_DECODE_PADDING = 16;

pub const BlockHeader = struct {
    num_items: u16,
    docid_list_offset: u16,
    first_hash: u32,
};

pub fn encodeBlockHeader(header: BlockHeader, out_data: []u8) void {
    std.debug.assert(out_data.len >= BLOCK_HEADER_SIZE);
    std.mem.writeInt(u16, out_data[0..2], header.num_items, .little);
    std.mem.writeInt(u16, out_data[2..4], header.docid_list_offset, .little);
    std.mem.writeInt(u32, out_data[4..8], header.first_hash, .little);
}

pub fn decodeBlockHeader(data: []const u8) BlockHeader {
    std.debug.assert(data.len >= BLOCK_HEADER_SIZE);
    return BlockHeader{
        .num_items = std.mem.readInt(u16, data[0..2], .little),
        .docid_list_offset = std.mem.readInt(u16, data[2..4], .little),
        .first_hash = std.mem.readInt(u32, data[4..8], .little),
    };
}

pub fn svbDecodeQuad0124(in_control: u8, in_data: []const u8, out: []u32) usize {
    std.debug.assert(out.len >= 4);
    std.debug.assert(in_data.len >= SIMD_DECODE_PADDING); // SIMD implementation requires padding
    return c.svb_decode_quad_0124(in_control, in_data.ptr, out.ptr);
}

pub fn svbDecodeQuad1234(in_control: u8, in_data: []const u8, out: []u32) usize {
    std.debug.assert(out.len >= 4);
    std.debug.assert(in_data.len >= SIMD_DECODE_PADDING); // SIMD implementation requires padding
    return c.svb_decode_quad_1234(in_control, in_data.ptr, out.ptr);
}

pub fn decodeBlockHashes(header: BlockHeader, in: []const u8, out: []u32) usize {
    const num_quads = (header.num_items + 3) / 4;

    var in_ptr = in[BLOCK_HEADER_SIZE..];
    var in_control_ptr = in_ptr[0..num_quads];
    var in_data_ptr = in_ptr[num_quads..];

    var out_ptr = out;

    var remaining = header.num_items;
    while (remaining >= 4) {
        const consumed = svbDecodeQuad0124(in_control_ptr[0], in_data_ptr, out_ptr);
        in_control_ptr = in_control_ptr[1..];
        in_data_ptr = in_data_ptr[consumed..];
        out_ptr = out_ptr[4..];
        remaining -= 4;
    }

    if (remaining > 0) {
        const consumed = svbDecodeQuad0124(in_control_ptr[0], in_data_ptr, out_ptr);
        in_control_ptr = in_control_ptr[1..];
        in_data_ptr = in_data_ptr[consumed..];
        out_ptr = out_ptr[remaining..];
        remaining = 0;
    }

    // Apply delta decoding - first item is absolute, rest are deltas
    out[0] += header.first_hash;
    for (1..header.num_items) |i| {
        out[i] += out[i - 1];
    }

    return out.len - out_ptr.len;
}

pub fn decodeBlockDocids(header: BlockHeader, hashes: []const u32, in: []const u8, min_doc_id: u32, out: []u32) usize {
    const num_quads = (header.num_items + 3) / 4;

    var in_ptr = in[BLOCK_HEADER_SIZE + header.docid_list_offset ..];
    var in_control_ptr = in_ptr[0..num_quads];
    var in_data_ptr = in_ptr[num_quads..];

    var out_ptr = out;

    std.debug.assert(out.len >= header.num_items);

    var remaining = header.num_items;
    while (remaining >= 4) {
        const consumed = svbDecodeQuad1234(in_control_ptr[0], in_data_ptr, out_ptr);
        in_control_ptr = in_control_ptr[1..];
        in_data_ptr = in_data_ptr[consumed..];
        out_ptr = out_ptr[4..];
        remaining -= 4;
    }

    if (remaining > 0) {
        const consumed = svbDecodeQuad1234(in_control_ptr[0], in_data_ptr, out_ptr);
        in_control_ptr = in_control_ptr[1..];
        in_data_ptr = in_data_ptr[consumed..];
        out_ptr = out_ptr[remaining..];
        remaining = 0;
    }

    // First item is always absolute, add min_doc_id back
    if (header.num_items > 0) {
        out[0] = out[0] + min_doc_id;
    }

    // Apply delta decoding for docids and add min_doc_id back to absolute values
    for (1..header.num_items) |i| {
        if (hashes[i] == hashes[i - 1]) {
            // Same hash - this is a delta, just add to previous
            out[i] = out[i] + out[i - 1];
        } else {
            // Different hash - this was an absolute value, add min_doc_id back
            out[i] = out[i] + min_doc_id;
        }
    }

    return out.len - out_ptr.len;
}

// Encode single value into a StreamVByte encoded byte array.
/// Encodes a single 32-bit integer using the StreamVByte "0124" variant,
/// where the control byte uses bits to indicate the encoded size:
/// 0 bytes for zero, 1 byte for <256, 2 bytes for <65536, 4 bytes otherwise.
pub fn svbEncodeValue0124(in: u32, out_data: []u8, out_control: *u8, comptime index: u8) usize {
    if (in == 0) {
        out_control.* |= 0 << (2 * index);
        return 0;
    } else if (in < (1 << 8)) {
        std.mem.writeInt(u8, out_data[0..1], @intCast(in), .little);
        out_control.* |= 1 << (2 * index);
        return 1;
    } else if (in < (1 << 16)) {
        std.mem.writeInt(u16, out_data[0..2], @intCast(in), .little);
        out_control.* |= 2 << (2 * index);
        return 2;
    } else {
        std.mem.writeInt(u32, out_data[0..4], in, .little);
        out_control.* |= 3 << (2 * index);
        return 4;
    }
}

// Encodes a 32-bit integer into a StreamVByte format using the 1/2/3/4-byte variant.
// Control byte is updated to indicate the number of bytes used for encoding.
// Returns the number of bytes written to out_data.
pub fn svbEncodeValue1234(in: u32, out_data: []u8, out_control: *u8, comptime index: u8) usize {
    if (in < (1 << 8)) {
        std.mem.writeInt(u8, out_data[0..1], @intCast(in), .little);
        out_control.* |= 0 << (2 * index);
        return 1;
    } else if (in < (1 << 16)) {
        std.mem.writeInt(u16, out_data[0..2], @intCast(in), .little);
        out_control.* |= 1 << (2 * index);
        return 2;
    } else if (in < (1 << 24)) {
        std.mem.writeInt(u24, out_data[0..3], @intCast(in), .little);
        out_control.* |= 2 << (2 * index);
        return 3;
    } else {
        std.mem.writeInt(u32, out_data[0..4], in, .little);
        out_control.* |= 3 << (2 * index);
        return 4;
    }
}

// Encode four 32-bit integers into a StreamVByte encoded byte array. (0124 variant)
pub fn svbEncodeQuad0124(in: [4]u32, out_data: []u8, out_control: *u8) usize {
    var out_data_ptr = out_data;
    out_control.* = 0;
    inline for (0..4) |i| {
        const size = svbEncodeValue0124(in[i], out_data_ptr, out_control, i);
        out_data_ptr = out_data_ptr[size..];
    }
    return out_data.len - out_data_ptr.len;
}

// Encode four 32-bit integers into a StreamVByte encoded byte array. (1234 variant)
pub fn svbEncodeQuad1234(in: [4]u32, out_data: []u8, out_control: *u8) usize {
    var out_data_ptr = out_data;
    out_control.* = 0; // Reset control byte
    inline for (0..4) |i| {
        const size = svbEncodeValue1234(in[i], out_data_ptr, out_control, i);
        out_data_ptr = out_data_ptr[size..];
    }
    return out_data.len - out_data_ptr.len;
}

pub const BlockEncoder = struct {
    num_items: u16 = 0,
    last_hash: u32 = 0,
    last_docid: u32 = 0,

    out_hashes: [MAX_BLOCK_SIZE]u8 = undefined,
    out_hashes_control: [MAX_BLOCK_SIZE]u8 = undefined,

    out_docids: [MAX_BLOCK_SIZE]u8 = undefined,
    out_docids_control: [MAX_BLOCK_SIZE]u8 = undefined,

    out_hashes_len: usize = 0,
    out_hashes_control_len: usize = 0,

    out_docids_len: usize = 0,
    out_docids_control_len: usize = 0,

    const Buffer = std.BoundedArray(u32, MAX_ITEMS_PER_BLOCK);

    const Self = @This();

    pub fn init() Self {
        return .{};
    }

    pub fn encodeChunk(self: *Self, items: []const Item, min_doc_id: u32, block_size: usize, comptime full_chunk: bool) !void {
        std.debug.assert(items.len > 0);
        std.debug.assert(items.len <= 4);

        if (full_chunk) {
            std.debug.assert(items.len == 4);
        }

        var chunk_hashes: [4]u32 = undefined;
        var chunk_docids: [4]u32 = undefined;

        for (0..items.len) |i| {
            // Calculate hash delta from previous item
            std.debug.assert(items[i].hash >= self.last_hash);
            chunk_hashes[i] = items[i].hash - self.last_hash;
            if (chunk_hashes[i] == 0) {
                // Same hash, encode docid delta
                std.debug.assert(items[i].id >= self.last_docid);
                chunk_docids[i] = items[i].id - self.last_docid;
            } else {
                // Different hash, encode absolute docid minus min_doc_id
                std.debug.assert(items[i].id >= min_doc_id);
                chunk_docids[i] = items[i].id - min_doc_id;
            }
            self.last_hash = items[i].hash;
            self.last_docid = items[i].id;
        }
        for (items.len..4) |i| {
            chunk_hashes[i] = 0; // Fill with zeroes for partial chunks
            chunk_docids[i] = 0;
        }

        const encoded_hash_size = svbEncodeQuad0124(
            chunk_hashes,
            self.out_hashes[self.out_hashes_len..],
            &self.out_hashes_control[self.out_hashes_control_len],
        );

        const encoded_docid_size = svbEncodeQuad1234(
            chunk_docids,
            self.out_docids[self.out_docids_len..],
            &self.out_docids_control[self.out_docids_control_len],
        );

        const new_out_hashes_len = self.out_hashes_len + encoded_hash_size;
        const new_out_hashes_control_len = self.out_hashes_control_len + 1;
        const new_out_docids_len = self.out_docids_len + encoded_docid_size;
        const new_out_docids_control_len = self.out_docids_control_len + 1;

        const new_block_size = BLOCK_HEADER_SIZE + new_out_hashes_len + new_out_hashes_control_len + new_out_docids_len + new_out_docids_control_len;

        if (new_block_size > block_size) {
            return error.BlockFull;
        }

        self.out_hashes_len = new_out_hashes_len;
        self.out_hashes_control_len = new_out_hashes_control_len;
        self.out_docids_len = new_out_docids_len;
        self.out_docids_control_len = new_out_docids_control_len;
        self.num_items += if (full_chunk) 4 else @intCast(items.len);
    }

    /// Encode items into a block and return the number of items consumed.
    /// Takes more items than needed to fill one block, always returns a full block.
    /// Returns the number of items consumed from the input.
    /// min_doc_id is subtracted from absolute docid values to reduce storage size.
    pub fn encodeBlock(self: *Self, items: []const Item, min_doc_id: u32, out: []u8) usize {
        const block_size = out.len;

        if (items.len == 0) {
            @memset(out, 0);
            return 0;
        }

        const first_hash = items[0].hash;

        // Reset encoder state for this block
        self.num_items = 0;
        self.last_hash = first_hash;
        self.last_docid = min_doc_id;
        self.out_hashes_len = 0;
        self.out_hashes_control_len = 0;
        self.out_docids_len = 0;
        self.out_docids_control_len = 0;

        // Try to encode items in chunks of 4
        var items_ptr = items;
        while (items_ptr.len >= 4) {
            self.encodeChunk(items_ptr[0..4], min_doc_id, block_size, true) catch {
                items_ptr = items_ptr[0..0];
                break;
            };
            items_ptr = items_ptr[4..];
        }

        // Try to encode remaining items in partial chunk (max 3 items)
        if (items_ptr.len > 0) {
            self.encodeChunk(items_ptr, min_doc_id, block_size, false) catch {};
        }

        // Write the block
        const header = BlockHeader{
            .num_items = self.num_items,
            .first_hash = first_hash,
            .docid_list_offset = @intCast(self.out_hashes_len + self.out_hashes_control_len),
        };

        var out_ptr = out;

        encodeBlockHeader(header, out_ptr);
        out_ptr = out_ptr[BLOCK_HEADER_SIZE..];

        @memcpy(out_ptr[0..self.out_hashes_control_len], self.out_hashes_control[0..self.out_hashes_control_len]);
        out_ptr = out_ptr[self.out_hashes_control_len..];

        @memcpy(out_ptr[0..self.out_hashes_len], self.out_hashes[0..self.out_hashes_len]);
        out_ptr = out_ptr[self.out_hashes_len..];

        @memcpy(out_ptr[0..self.out_docids_control_len], self.out_docids_control[0..self.out_docids_control_len]);
        out_ptr = out_ptr[self.out_docids_control_len..];

        @memcpy(out_ptr[0..self.out_docids_len], self.out_docids[0..self.out_docids_len]);
        out_ptr = out_ptr[self.out_docids_len..];

        // Zero out remaining space
        @memset(out_ptr, 0);

        return self.num_items;
    }
};

test "encodeBlockHeader/decodeBlockHeader" {
    const header = BlockHeader{
        .num_items = 10,
        .docid_list_offset = 20,
        .first_hash = 12345678,
    };
    var buffer: [BLOCK_HEADER_SIZE]u8 = undefined;
    encodeBlockHeader(header, buffer[0..]);

    const decoded_header = decodeBlockHeader(buffer[0..]);
    try std.testing.expectEqual(header.num_items, decoded_header.num_items);
    try std.testing.expectEqual(header.docid_list_offset, decoded_header.docid_list_offset);
    try std.testing.expectEqual(header.first_hash, decoded_header.first_hash);
}

test "BlockEncoder" {
    var encoder = BlockEncoder.init();
    const items: []const Item = &.{
        .{ .hash = 1, .id = 100 },
        .{ .hash = 1, .id = 200 },
        .{ .hash = 3, .id = 300 },
        .{ .hash = 4, .id = 400 },
        .{ .hash = 5, .id = 500 },
    };

    const min_doc_id: u32 = 50;
    var block: [64]u8 = undefined;
    const consumed = encoder.encodeBlock(items, min_doc_id, &block);
    try std.testing.expectEqual(5, consumed);

    const header = decodeBlockHeader(&block);
    try std.testing.expectEqual(5, header.num_items);
    try std.testing.expectEqual(1, header.first_hash);

    var hashes: [MAX_ITEMS_PER_BLOCK]u32 = undefined;
    const num_hashes = decodeBlockHashes(header, &block, &hashes);
    try std.testing.expectEqual(5, num_hashes);
    try std.testing.expectEqualSlices(u32, &[_]u32{ 1, 1, 3, 4, 5 }, hashes[0..num_hashes]);

    var docids: [MAX_ITEMS_PER_BLOCK]u32 = undefined;
    const num_docids = decodeBlockDocids(header, hashes[0..num_hashes], &block, min_doc_id, &docids);
    try std.testing.expectEqual(5, num_docids);
    try std.testing.expectEqualSlices(u32, &[_]u32{ 100, 200, 300, 400, 500 }, docids[0..num_docids]);
}
