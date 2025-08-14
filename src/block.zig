const std = @import("std");
const assert = std.debug.assert;

const streamvbyte = @import("streamvbyte.zig");
const Item = @import("segment.zig").Item;

// Block handling for compressed (hash, docid) pairs using StreamVByte encoding.
// Each block has a fixed size and are written into a file, they need to be fixed size for easier indexing.
//
// Block format:
//  - u16   num items
//  - u16   docid list offset
//  - u32   first hash
//  - []u8  encoded hash list
//  - []u8  padding
//  - []u8  encoded docid list

// Block-related constants
pub const MIN_BLOCK_SIZE = 64;
pub const MAX_BLOCK_SIZE = 4096;
pub const MAX_ITEMS_PER_BLOCK = MAX_BLOCK_SIZE / 2;
pub const BLOCK_HEADER_SIZE = 8; // u16 + u16 + u32


pub const BlockHeader = struct {
    num_items: u16,
    docid_list_offset: u16,
    first_hash: u32,
};

/// Decode a BlockHeader from bytes
pub fn decodeBlockHeader(data: []const u8) BlockHeader {
    std.debug.assert(data.len >= BLOCK_HEADER_SIZE);
    return BlockHeader{
        .num_items = std.mem.readInt(u16, data[0..2], .little),
        .docid_list_offset = std.mem.readInt(u16, data[2..4], .little),
        .first_hash = std.mem.readInt(u32, data[4..8], .little),
    };
}

/// Encode a BlockHeader into bytes
pub fn encodeBlockHeader(header: BlockHeader, out_data: []u8) void {
    std.debug.assert(out_data.len >= BLOCK_HEADER_SIZE);
    std.mem.writeInt(u16, out_data[0..2], header.num_items, .little);
    std.mem.writeInt(u16, out_data[2..4], header.docid_list_offset, .little);
    std.mem.writeInt(u32, out_data[4..8], header.first_hash, .little);
}

/// BlockReader efficiently searches for hashes within a single compressed block
/// Caches decompressed data to avoid redundant decompression when searching multiple hashes
pub const BlockReader = struct {
    // Configuration
    min_doc_id: u32,

    // Block data (set via load())
    block_data: ?[]const u8 = null,

    // Cached decompressed data
    block_header: BlockHeader = .{ .num_items = 0, .docid_list_offset = 0, .first_hash = 0 },
    hashes: [MAX_ITEMS_PER_BLOCK]u32 = undefined,
    docids: [MAX_ITEMS_PER_BLOCK]u32 = undefined,

    // State tracking
    header_loaded: bool = false,
    hashes_loaded: bool = false,
    docids_loaded: bool = false,

    const Self = @This();

    const HashRange = struct {
        start: usize,
        end: usize,
    };

    /// Initialize a reusable BlockReader with the given min_doc_id
    pub fn init(min_doc_id: u32) BlockReader {
        return BlockReader{
            .min_doc_id = min_doc_id,
        };
    }

    /// Load a new block for searching
    /// This resets all cached state and loads the block header
    pub fn load(self: *BlockReader, block_data: []const u8, lazy: bool) void {
        assert(block_data.len >= BLOCK_HEADER_SIZE);

        // Reset all state
        self.block_data = block_data;
        self.header_loaded = false;
        self.hashes_loaded = false;
        self.docids_loaded = false;

        // Load header immediately
        self.block_header = decodeBlockHeader(block_data);
        self.header_loaded = true;

        // If full is true, decode all hashes and docids immediately
        if (!lazy) {
            self.ensureHashesLoaded();
            self.ensureDocidsLoaded();
        }
    }

    /// Check if a block is currently loaded
    pub fn isLoaded(self: *const BlockReader) bool {
        return self.block_data != null and self.header_loaded;
    }

    /// Check if the block is empty (no items)
    pub fn isEmpty(self: *const BlockReader) bool {
        return self.block_header.num_items == 0;
    }

    /// Reset the searcher state (clears cached data but keeps block_data)
    pub fn reset(self: *BlockReader) void {
        self.hashes_loaded = false;
        self.docids_loaded = false;
        // Keep header_loaded and block_data as they're still valid
    }

    /// Load and cache hashes if not already loaded
    fn ensureHashesLoaded(self: *BlockReader) void {
        if (self.hashes_loaded) return;

        if (self.isEmpty()) {
            self.hashes_loaded = true;
            return;
        }

        const num_hashes = decodeBlockHashes(self.block_header, self.block_data.?, &self.hashes);
        assert(num_hashes == self.block_header.num_items);
        self.hashes_loaded = true;
    }

    /// Load and cache docids if not already loaded
    fn ensureDocidsLoaded(self: *BlockReader) void {
        if (self.docids_loaded) return;

        self.ensureHashesLoaded(); // Docids decoding needs hashes
        if (self.isEmpty()) {
            self.docids_loaded = true;
            return;
        }

        const num_docids = decodeBlockDocids(self.block_header, self.hashes[0..self.block_header.num_items], self.block_data.?, self.min_doc_id, &self.docids);
        assert(num_docids == self.block_header.num_items);
        self.docids_loaded = true;
    }

    /// Get the range of the first hash in this block (for block-level filtering)
    pub fn getFirstHash(self: *BlockReader) u32 {
        return self.block_header.first_hash;
    }

    /// Get the number of items in this block
    pub fn getNumItems(self: *BlockReader) u16 {
        return self.block_header.num_items;
    }

    /// Find all occurrences of a hash in this block
    /// Returns the range [start, end) of matching indices
    pub fn findHash(self: *BlockReader, hash: u32) HashRange {
        if (self.isEmpty()) {
            return HashRange{ .start = 0, .end = 0 };
        }

        // Ensure hashes are loaded
        self.ensureHashesLoaded();

        // Binary search for the hash range
        const hashes_slice = self.hashes[0..self.block_header.num_items];
        const range = std.sort.equalRange(u32, hashes_slice, hash, compareHashes);
        return HashRange{ .start = range[0], .end = range[1] };
    }

    /// Get docids for a specific range (typically from findHash result)
    /// Caller must ensure range is valid
    pub fn getDocidsForRange(self: *BlockReader, range: HashRange) []const u32 {
        if (range.start >= range.end) {
            return &[_]u32{};
        }

        // Always use optimized range decoding to avoid decompressing entire block
        self.ensureHashesLoaded();
        const num_decoded = decodeBlockDocidsRange(
            self.block_header,
            self.hashes[0..self.block_header.num_items],
            self.block_data.?,
            self.min_doc_id,
            range.start,
            range.end,
            self.docids[0..] // Pass full array for proper SIMD padding
        );
        std.debug.assert(num_decoded == range.end - range.start);
        return self.docids[range.start..range.end];
    }

    /// Convenience method: find hash and return corresponding docids
    pub fn searchHash(self: *BlockReader, hash: u32) []const u32 {
        const range = self.findHash(hash);
        return self.getDocidsForRange(range);
    }

    pub fn getHashes(self: *BlockReader) []const u32 {
        self.ensureHashesLoaded();
        return self.hashes[0..self.block_header.num_items];
    }

    pub fn getDocids(self: *BlockReader) []const u32 {
        self.ensureDocidsLoaded();
        return self.docids[0..self.block_header.num_items];
    }

    fn compareHashes(a: u32, b: u32) std.math.Order {
        return std.math.order(a, b);
    }
};

// Tests
const testing = std.testing;

test "BlockReader basic functionality" {
    // Create a simple test block using BlockEncoder
    var encoder = BlockEncoder.init();
    const items = [_]@import("segment.zig").Item{
        .{ .hash = 100, .id = 1 },
        .{ .hash = 100, .id = 2 },
        .{ .hash = 200, .id = 3 },
        .{ .hash = 300, .id = 4 },
    };

    const min_doc_id: u32 = 1;
    var block_data: [256]u8 = undefined;
    const num_items = encoder.encodeBlock(&items, min_doc_id, &block_data);
    try testing.expectEqual(4, num_items);

    // Test BlockReader
    var reader = BlockReader.init(min_doc_id);
    reader.load(&block_data, false);

    // Test basic properties
    try testing.expectEqual(@as(u16, 4), reader.getNumItems());
    try testing.expectEqual(@as(u32, 100), reader.getFirstHash());
    try testing.expectEqual(false, reader.isEmpty());

    // Test findHash
    const range100 = reader.findHash(100);
    try testing.expectEqual(@as(usize, 0), range100.start);
    try testing.expectEqual(@as(usize, 2), range100.end);

    const range200 = reader.findHash(200);
    try testing.expectEqual(@as(usize, 2), range200.start);
    try testing.expectEqual(@as(usize, 3), range200.end);

    const range404 = reader.findHash(404);
    try testing.expectEqual(@as(usize, 4), range404.start);
    try testing.expectEqual(@as(usize, 4), range404.end);

    // Test getDocidsForRange
    const docids100 = reader.getDocidsForRange(range100);
    try testing.expectEqual(@as(usize, 2), docids100.len);
    try testing.expectEqual(@as(u32, 1), docids100[0]);
    try testing.expectEqual(@as(u32, 2), docids100[1]);

    const docids200 = reader.getDocidsForRange(range200);
    try testing.expectEqual(@as(usize, 1), docids200.len);
    try testing.expectEqual(@as(u32, 3), docids200[0]);

    // Test searchHash convenience method
    const docids_direct = reader.searchHash(100);
    try testing.expectEqualSlices(u32, docids100, docids_direct);
}

test "BlockReader range-based docid decoding" {
    // Create a test block with multiple hashes
    var encoder = BlockEncoder.init();
    const items = [_]@import("segment.zig").Item{
        .{ .hash = 100, .id = 1001 },
        .{ .hash = 100, .id = 1005 },
        .{ .hash = 100, .id = 1010 },
        .{ .hash = 200, .id = 2001 },
        .{ .hash = 200, .id = 2002 },
        .{ .hash = 300, .id = 3001 },
        .{ .hash = 300, .id = 3002 },
        .{ .hash = 300, .id = 3003 },
    };

    const min_doc_id: u32 = 1000;
    var block_data: [512]u8 = undefined;
    const num_items = encoder.encodeBlock(&items, min_doc_id, &block_data);
    try testing.expectEqual(8, num_items);

    // Test BlockReader with range optimization
    var reader = BlockReader.init(min_doc_id);
    reader.load(&block_data, false);

    // Test small range (should use optimization)
    const range100 = reader.findHash(100);
    try testing.expectEqual(@as(usize, 0), range100.start);
    try testing.expectEqual(@as(usize, 3), range100.end);
    
    const docids100 = reader.getDocidsForRange(range100);
    try testing.expectEqual(@as(usize, 3), docids100.len);
    try testing.expectEqual(@as(u32, 1001), docids100[0]);
    try testing.expectEqual(@as(u32, 1005), docids100[1]);
    try testing.expectEqual(@as(u32, 1010), docids100[2]);

    // Test another small range
    const range200 = reader.findHash(200);
    try testing.expectEqual(@as(usize, 3), range200.start);
    try testing.expectEqual(@as(usize, 5), range200.end);
    
    const docids200 = reader.getDocidsForRange(range200);
    try testing.expectEqual(@as(usize, 2), docids200.len);
    try testing.expectEqual(@as(u32, 2001), docids200[0]);
    try testing.expectEqual(@as(u32, 2002), docids200[1]);

    // Test range at end
    const range300 = reader.findHash(300);
    try testing.expectEqual(@as(usize, 5), range300.start);
    try testing.expectEqual(@as(usize, 8), range300.end);
    
    const docids300 = reader.getDocidsForRange(range300);
    try testing.expectEqual(@as(usize, 3), docids300.len);
    try testing.expectEqual(@as(u32, 3001), docids300[0]);
    try testing.expectEqual(@as(u32, 3002), docids300[1]);
    try testing.expectEqual(@as(u32, 3003), docids300[2]);
}

/// BlockEncoder handles encoding of (hash, docid) items into compressed blocks
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

        const encoded_hash_size = streamvbyte.svbEncodeQuad0124(
            chunk_hashes,
            self.out_hashes[self.out_hashes_len..],
            &self.out_hashes_control[self.out_hashes_control_len],
        );

        const encoded_docid_size = streamvbyte.svbEncodeQuad1234(
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
    try testing.expectEqual(header.num_items, decoded_header.num_items);
    try testing.expectEqual(header.docid_list_offset, decoded_header.docid_list_offset);
    try testing.expectEqual(header.first_hash, decoded_header.first_hash);
}

test "BlockEncoder basic functionality" {
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
    try testing.expectEqual(5, consumed);

    const header = decodeBlockHeader(&block);
    try testing.expectEqual(5, header.num_items);
    try testing.expectEqual(1, header.first_hash);

    var hashes: [MAX_ITEMS_PER_BLOCK]u32 = undefined;
    const num_hashes = decodeBlockHashes(header, &block, &hashes);
    try testing.expectEqual(5, num_hashes);
    try testing.expectEqualSlices(u32, &[_]u32{ 1, 1, 3, 4, 5 }, hashes[0..num_hashes]);

    var docids: [MAX_ITEMS_PER_BLOCK]u32 = undefined;
    const num_docids = decodeBlockDocids(header, hashes[0..num_hashes], &block, min_doc_id, &docids);
    try testing.expectEqual(5, num_docids);
    try testing.expectEqualSlices(u32, &[_]u32{ 100, 200, 300, 400, 500 }, docids[0..num_docids]);
}

pub fn decodeBlockHashes(header: BlockHeader, in: []const u8, out: []u32) usize {
    // Read StreamVByte-encoded deltas
    const offset = BLOCK_HEADER_SIZE;
    const num_decoded = streamvbyte.decodeValues(
        header.num_items,
        in[offset..],
        out,
        streamvbyte.Variant.variant0124,
    );

    // Apply delta decoding - first item is absolute, rest are deltas
    streamvbyte.svbDeltaDecodeInPlace(out[0..header.num_items], header.first_hash);

    return num_decoded;
}

pub fn decodeBlockDocids(header: BlockHeader, hashes: []const u32, in: []const u8, min_doc_id: u32, out: []u32) usize {
    // Read StreamVByte-encoded docids
    const offset = BLOCK_HEADER_SIZE + header.docid_list_offset;
    const num_decoded = streamvbyte.decodeValues(
        header.num_items,
        in[offset..],
        out,
        streamvbyte.Variant.variant1234,
    );

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

    return num_decoded;
}

// Decode docids for a specific range within a block for a single hash
// ASSUMPTION: start_idx must be at a hash boundary (start_idx == 0 OR hashes[start_idx] != hashes[start_idx-1])
// AND all items in the range [start_idx, end_idx) have the same hash value
// This is guaranteed when called with ranges from findHash()
pub fn decodeBlockDocidsRange(header: BlockHeader, hashes: []const u32, in: []const u8, min_doc_id: u32, start_idx: usize, end_idx: usize, out: []u32) usize {
    const actual_end = @min(end_idx, header.num_items);
    if (start_idx >= actual_end) return 0;
    
    // Debug assertion to verify our hash boundary assumption
    std.debug.assert(start_idx == 0 or hashes[start_idx] != hashes[start_idx - 1]);
    
    const offset = BLOCK_HEADER_SIZE + header.docid_list_offset;
    
    // Decode the range directly into the output array
    _ = streamvbyte.decodeValuesRange(
        header.num_items,
        start_idx,
        actual_end,
        in[offset..],
        out,
        streamvbyte.Variant.variant1234
    );
    
    // Apply delta decoding to the range
    // Since all items have the same hash and we start at a hash boundary,
    // the first item is absolute, all subsequent items are deltas
    const range_size = actual_end - start_idx;
    out[start_idx] = out[start_idx] + min_doc_id;
    
    // Apply delta decoding to rest of range - all are deltas since same hash
    for (1..range_size) |i| {
        const global_idx = start_idx + i;
        out[global_idx] = out[global_idx] + out[global_idx - 1];
    }
    
    return range_size;
}
