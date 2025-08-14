const std = @import("std");
const assert = std.debug.assert;

const streamvbyte = @import("streamvbyte.zig");
const Item = @import("segment.zig").Item;

// Block handling for compressed (hash, docid) pairs using StreamVByte encoding.
// Each block has a fixed size and are written into a file, they need to be fixed size for easier indexing.
//
// Block format:
//  - u32   first hash
//  - u16   num unique hashes
//  - u16   num items
//  - u16   counts offset
//  - u16   docid list offset
//  - []u8  encoded unique hash deltas
//  - []u8  encoded hash counts
//  - []u8  encoded docid deltas

// Block-related constants
pub const MIN_BLOCK_SIZE = 64;
pub const MAX_BLOCK_SIZE = 4096;
pub const MAX_ITEMS_PER_BLOCK = MAX_BLOCK_SIZE / 2;
pub const BLOCK_HEADER_SIZE = 12; // u32 + u16 + u16 + u16 + u16

pub const BlockHeader = struct {
    first_hash: u32,
    num_hashes: u16,
    num_items: u16,
    counts_offset: u16,
    docids_offset: u16,
};

/// Decode a BlockHeader from bytes
pub fn decodeBlockHeader(data: []const u8) BlockHeader {
    std.debug.assert(data.len >= BLOCK_HEADER_SIZE);
    return BlockHeader{
        .first_hash = std.mem.readInt(u32, data[0..4], .little),
        .num_hashes = std.mem.readInt(u16, data[4..6], .little),
        .num_items = std.mem.readInt(u16, data[6..8], .little),
        .counts_offset = std.mem.readInt(u16, data[8..10], .little),
        .docids_offset = std.mem.readInt(u16, data[10..12], .little),
    };
}

/// Encode a BlockHeader into bytes
pub fn encodeBlockHeader(header: BlockHeader, out_data: []u8) void {
    std.debug.assert(out_data.len >= BLOCK_HEADER_SIZE);
    std.mem.writeInt(u32, out_data[0..4], header.first_hash, .little);
    std.mem.writeInt(u16, out_data[4..6], header.num_hashes, .little);
    std.mem.writeInt(u16, out_data[6..8], header.num_items, .little);
    std.mem.writeInt(u16, out_data[8..10], header.counts_offset, .little);
    std.mem.writeInt(u16, out_data[10..12], header.docids_offset, .little);
}

/// BlockReader efficiently searches for hashes within a single compressed block
/// Caches decompressed data to avoid redundant decompression when searching multiple hashes
pub const BlockReader = struct {
    // Configuration
    min_doc_id: u32,

    // Block data (set via load())
    block_data: ?[]const u8 = null,

    // Cached decompressed data
    block_header: BlockHeader = .{ .first_hash = 0, .num_hashes = 0, .num_items = 0, .counts_offset = 0, .docids_offset = 0 },
    hashes: [MAX_ITEMS_PER_BLOCK]u32 = undefined,
    counts: [MAX_ITEMS_PER_BLOCK]u32 = undefined,
    docids: [MAX_ITEMS_PER_BLOCK]u32 = undefined,

    // State tracking
    hashes_loaded: bool = false,
    counts_loaded: bool = false,
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
        self.hashes_loaded = false;
        self.counts_loaded = false;
        self.docids_loaded = false;

        // Load header immediately
        self.block_header = decodeBlockHeader(block_data);

        // If full is true, decode all hashes, counts and docids immediately
        if (!lazy) {
            self.ensureHashesLoaded();
            self.ensureCountsLoaded();
            self.ensureDocidsLoaded();
        }
    }

    /// Check if a block is currently loaded
    pub fn isLoaded(self: *const BlockReader) bool {
        return self.block_data != null and self.header_loaded;
    }

    /// Check if the block is empty (no items)
    pub fn isEmpty(self: *const BlockReader) bool {
        return self.block_header.num_hashes == 0;
    }

    /// Reset the searcher state (clears cached data but keeps block_data)
    pub fn reset(self: *BlockReader) void {
        self.hashes_loaded = false;
        self.counts_loaded = false;
        self.docids_loaded = false;
        // Keep header_loaded and block_data as they're still valid
    }

    /// Load and cache unique hashes if not already loaded
    fn ensureHashesLoaded(self: *BlockReader) void {
        if (self.hashes_loaded) return;

        if (self.isEmpty()) {
            self.hashes_loaded = true;
            return;
        }

        const offset = BLOCK_HEADER_SIZE;
        streamvbyte.decodeValues(
            self.block_header.num_hashes,
            0,
            self.block_header.num_hashes,
            self.block_data.?[offset..],
            &self.hashes,
            .variant1234,
        );
        streamvbyte.svbDeltaDecodeInPlace(self.hashes[0..self.block_header.num_hashes], self.block_header.first_hash);
        self.hashes_loaded = true;
    }

    /// Load and cache counts as offsets if not already loaded
    fn ensureCountsLoaded(self: *BlockReader) void {
        if (self.counts_loaded) return;

        if (self.isEmpty()) {
            self.counts_loaded = true;
            return;
        }

        const offset = BLOCK_HEADER_SIZE + self.block_header.counts_offset;
        streamvbyte.decodeValues(
            self.block_header.num_hashes,
            0,
            self.block_header.num_hashes,
            self.block_data.?[offset..],
            &self.counts,
            .variant1234,
        );
        streamvbyte.svbDeltaDecodeInPlace(self.counts[0..self.block_header.num_hashes], 0);
        self.counts_loaded = true;
    }

    /// Load and cache docids if not already loaded
    fn ensureDocidsLoaded(self: *BlockReader) void {
        if (self.docids_loaded) return;

        if (self.isEmpty()) {
            self.docids_loaded = true;
            return;
        }

        self.ensureHashesLoaded();
        self.ensureCountsLoaded();

        const offset = BLOCK_HEADER_SIZE + self.block_header.docids_offset;
        streamvbyte.decodeValues(
            self.block_header.num_items,
            0,
            self.block_header.num_items,
            self.block_data.?[offset..],
            &self.docids,
            .variant1234,
        );

        // Apply docid delta decoding similar to current approach
        var docid_idx: usize = 0;
        for (0..self.block_header.num_hashes) |hash_idx| {
            const count = if (hash_idx == 0) self.counts[0] else self.counts[hash_idx] - self.counts[hash_idx - 1];
            streamvbyte.svbDeltaDecodeInPlace(self.docids[docid_idx .. docid_idx + count], self.min_doc_id);
            docid_idx += count;
        }
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

        self.ensureHashesLoaded();

        // Binary search in unique hashes
        const hashes_slice = self.hashes[0..self.block_header.num_hashes];
        const hash_idx = std.sort.binarySearch(u32, hashes_slice, hash, orderU32);

        if (hash_idx == null) {
            // Hash not found - return empty range at end of items
            return HashRange{ .start = 0, .end = 0 };
        }

        self.ensureCountsLoaded();

        // O(1) range calculation from offsets
        const start = if (hash_idx.? == 0) 0 else self.counts[hash_idx.? - 1];
        const end = self.counts[hash_idx.?];
        return HashRange{ .start = start, .end = end };
    }

    /// Get docids for a specific range (typically from findHash result)
    /// Caller must ensure range is valid
    pub fn getDocidsForRange(self: *BlockReader, range: HashRange) []const u32 {
        if (range.start >= range.end) {
            return &[_]u32{};
        }

        // Read StreamVByte-encoded docids
        const offset = BLOCK_HEADER_SIZE + self.block_header.docids_offset;
        _ = streamvbyte.decodeValues(
            self.block_header.num_items,
            range.start,
            range.end,
            self.block_data.?[offset..],
            &self.docids,
            .variant1234,
        );

        const docids = self.docids[range.start..range.end];

        // Apply delta decoding for docids and add min_doc_id back to absolute values
        streamvbyte.svbDeltaDecodeInPlace(docids, self.min_doc_id);

        return docids;
    }

    /// Convenience method: find hash and return corresponding docids
    pub fn searchHash(self: *BlockReader, hash: u32) []const u32 {
        const range = self.findHash(hash);
        return self.getDocidsForRange(range);
    }

    pub fn getHashes(self: *BlockReader) []const u32 {
        self.ensureHashesLoaded();
        return self.hashes[0..self.block_header.num_hashes];
    }

    pub fn getDocids(self: *BlockReader) []const u32 {
        self.ensureDocidsLoaded();
        return self.docids[0..self.getNumItems()];
    }

    fn orderU32(lhs: u32, rhs: u32) std.math.Order {
        return std.math.order(lhs, rhs);
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
    const num_items = try encoder.encodeBlock(&items, min_doc_id, &block_data);
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
    try testing.expectEqual(BlockReader.HashRange{ .start = 0, .end = 2 }, range100);

    const range200 = reader.findHash(200);
    try testing.expectEqual(BlockReader.HashRange{ .start = 2, .end = 3 }, range200);

    const range404 = reader.findHash(404);
    try testing.expectEqual(BlockReader.HashRange{ .start = 0, .end = 0 }, range404);

    // Test getDocidsForRange
    const docids100 = reader.getDocidsForRange(range100);
    try std.testing.expectEqualSlices(u32, &[_]u32{ 1, 2 }, docids100);

    const docids200 = reader.getDocidsForRange(range200);
    try std.testing.expectEqualSlices(u32, &[_]u32{3}, docids200);

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
    const num_items = try encoder.encodeBlock(&items, min_doc_id, &block_data);
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
    last_hash: u32 = 0,
    last_docid: u32 = 0,

    out_header: BlockHeader = .{ .first_hash = 0, .num_hashes = 0, .num_items = 0, .counts_offset = 0, .docids_offset = 0 },

    // Unique hash buffering strategy:
    // Why 8 elements instead of encoding immediately at 4?
    //   1. Must check if entire chunk fits BEFORE committing (no partial chunks)
    //   2. When buffer has 4 hashes, next chunk might increment counts of existing hashes
    //   3. If next chunk doesn't fit, we'd need to rollback those count changes
    //   4. Easier to buffer up to 8, calculate total size, then encode if chunk fits
    //   5. When >4: encode first 4 hashes to output, keep remaining 4 for next batch
    // Worst case: 4 existing hashes + 4 new hashes from current chunk = 8 total
    num_buffered_hashes: usize = 0,
    buffered_hashes: [8]u32 = undefined,   // Delta-encoded hash values
    buffered_counts: [8]u32 = undefined,   // Count of items per unique hash

    out_hashes: std.BoundedArray(u8, MAX_BLOCK_SIZE) = .{},
    out_hashes_control: std.BoundedArray(u8, MAX_BLOCK_SIZE) = .{},

    out_counts: std.BoundedArray(u8, MAX_BLOCK_SIZE) = .{},
    out_counts_control: std.BoundedArray(u8, MAX_BLOCK_SIZE) = .{},

    out_docids: std.BoundedArray(u8, MAX_BLOCK_SIZE) = .{},
    out_docids_control: std.BoundedArray(u8, MAX_BLOCK_SIZE) = .{},

    const Buffer = std.BoundedArray(u32, MAX_ITEMS_PER_BLOCK);

    const Self = @This();

    pub fn init() Self {
        return .{};
    }

    fn flushTempBuffers(self: *Self) void {
        if (self.num_buffered_hashes == 0) {
            return; // Nothing to flush
        }

        // For the first set of 4, actually encode them
        const buffered_hashes_size = streamvbyte.svbEncodeQuad1234(
            self.buffered_hashes[0..4].*,
            self.out_hashes.unusedCapacitySlice(),
            &self.out_hashes_control.buffer[self.out_hashes_control.len],
        );
        self.out_hashes.len += buffered_hashes_size;
        self.out_hashes_control.len += 1;

        const buffered_counts_size = streamvbyte.svbEncodeQuad1234(
            self.buffered_counts[0..4].*,
            self.out_counts.unusedCapacitySlice(),
            &self.out_counts_control.buffer[self.out_counts_control.len],
        );
        self.out_counts.len += buffered_counts_size;
        self.out_counts_control.len += 1;

        self.out_header.num_hashes += @intCast(self.num_buffered_hashes);
        self.num_buffered_hashes = 0;
    }

    pub fn encodeChunk(self: *Self, items: []const Item, min_doc_id: u32, block_size: usize, comptime full_chunk: bool) !void {
        std.debug.assert(items.len > 0);
        std.debug.assert(items.len <= 4);

        if (full_chunk) {
            std.debug.assert(items.len == 4);
        }

        var num_buffered_hashes = self.num_buffered_hashes;
        var buffered_hashes: [8]u32 = self.buffered_hashes;
        var buffered_counts: [8]u32 = self.buffered_counts;

        var num_items = self.out_header.num_items;

        var chunk_docids: [4]u32 = .{ 0, 0, 0, 0 };

        for (0..items.len) |i| {
            const current_hash = items[i].hash;
            const current_docid = items[i].id;

            const is_new_hash = num_items == 0 or current_hash != self.last_hash;

            if (is_new_hash) {
                // New unique hash found - store as delta
                const delta = current_hash - self.last_hash;
                buffered_hashes[num_buffered_hashes] = delta;
                buffered_counts[num_buffered_hashes] = 1;
                num_buffered_hashes += 1;
            } else {
                // Same hash as last - increment count for current unique hash
                buffered_counts[num_buffered_hashes - 1] += 1;
            }

            if (!is_new_hash) {
                // Same hash, encode docid delta
                chunk_docids[i] = current_docid - self.last_docid;
            } else {
                // Different hash, encode absolute docid minus min_doc_id
                chunk_docids[i] = current_docid - min_doc_id;
            }

            self.last_hash = current_hash;
            self.last_docid = current_docid;
            num_items += 1;
        }

        const encoded_docid_size = streamvbyte.svbEncodeQuad1234(
            chunk_docids,
            self.out_docids.unusedCapacitySlice(),
            &self.out_docids_control.buffer[self.out_docids_control.len],
        );

        const new_out_docids_len = self.out_docids.len + encoded_docid_size;
        const new_out_docids_control_len = self.out_docids_control.len + 1;

        var new_out_hashes_len = self.out_hashes.len;
        var new_out_hashes_control_len = self.out_hashes_control.len;

        var new_out_counts_len = self.out_counts.len;
        var new_out_counts_control_len = self.out_counts_control.len;

        var buffered_hashes_size: usize = undefined;
        var buffered_counts_size: usize = undefined;

        // Handle buffered hash encoding based on buffer state:
        if (num_buffered_hashes <= 4) {
            // Buffer fits in one quad - just calculate size without encoding yet
            // (we'll encode later if this chunk fits in the block)
            buffered_hashes_size = streamvbyte.svbEncodeQuadSize1234(buffered_hashes[0..4].*) + 1;
            buffered_counts_size = streamvbyte.svbEncodeQuadSize1234(buffered_counts[0..4].*) + 1;
        } else {
            // Buffer overflow (>4) - must encode first quad now to free up space
            // Encode first 4 hashes/counts to output buffers
            const encoded_hashes_size = streamvbyte.svbEncodeQuad1234(
                buffered_hashes[0..4].*,
                self.out_hashes.unusedCapacitySlice(),
                &self.out_hashes_control.buffer[self.out_hashes_control.len],
            );
            new_out_hashes_len += encoded_hashes_size;
            new_out_hashes_control_len += 1;

            const encoded_counts_size = streamvbyte.svbEncodeQuad1234(
                buffered_counts[0..4].*,
                self.out_counts.unusedCapacitySlice(),
                &self.out_counts_control.buffer[self.out_counts_control.len],
            );
            new_out_counts_len += encoded_counts_size;
            new_out_counts_control_len += 1;

            // Calculate size for remaining 4 hashes (elements 4-7)
            buffered_hashes_size = streamvbyte.svbEncodeQuadSize1234(buffered_hashes[4..8].*) + 1;
            buffered_counts_size = streamvbyte.svbEncodeQuadSize1234(buffered_counts[4..8].*) + 1;
        }

        const new_block_size = BLOCK_HEADER_SIZE +
            new_out_hashes_len + new_out_hashes_control_len + buffered_hashes_size +
            new_out_counts_len + new_out_counts_control_len + buffered_counts_size +
            new_out_docids_len + new_out_docids_control_len;

        if (new_block_size > block_size) {
            return error.BlockFull;
        }

        self.out_header.num_items = num_items;

        self.out_hashes.len = new_out_hashes_len;
        self.out_hashes_control.len = new_out_hashes_control_len;

        self.out_counts.len = new_out_counts_len;
        self.out_counts_control.len = new_out_counts_control_len;

        self.out_docids.len = new_out_docids_len;
        self.out_docids_control.len = new_out_docids_control_len;

        // Update buffer state after successful chunk processing:
        if (num_buffered_hashes <= 4) {
            // All hashes still fit in buffer - just update buffer contents
            self.num_buffered_hashes = num_buffered_hashes;
            @memcpy(self.buffered_hashes[0..4], buffered_hashes[0..4]);
            @memcpy(self.buffered_counts[0..4], buffered_counts[0..4]);
        } else {
            // Buffer overflowed - first 4 were encoded, shift remaining 4 to front
            self.out_header.num_hashes += 4;  // Account for the 4 hashes we just encoded
            self.num_buffered_hashes = num_buffered_hashes - 4;  // Remaining buffered count
            @memcpy(self.buffered_hashes[0..4], buffered_hashes[4..8]);  // Shift elements 4-7 to 0-3
            @memcpy(self.buffered_counts[0..4], buffered_counts[4..8]);
        }
    }

    /// Encode items into a block and return the number of items consumed.
    /// Takes more items than needed to fill one block, always returns a full block.
    /// Returns the number of items consumed from the input.
    /// min_doc_id is subtracted from absolute docid values to reduce storage size.
    pub fn encodeBlock(self: *Self, items: []const Item, min_doc_id: u32, out: []u8) !usize {
        const block_size = out.len;

        if (items.len == 0) {
            @memset(out, 0);
            return 0;
        }

        const first_hash = items[0].hash;

        // Reset encoder state for this block
        self.out_header.num_items = 0;
        self.out_header.num_hashes = 0;

        self.last_hash = first_hash;
        self.last_docid = min_doc_id;

        self.num_buffered_hashes = 0;
        @memset(&self.buffered_hashes, 0);
        @memset(&self.buffered_counts, 0);

        self.out_hashes.clear();
        self.out_hashes_control.clear();
        self.out_counts.clear();
        self.out_counts_control.clear();
        self.out_docids.clear();
        self.out_docids_control.clear();

        // Try to encode items in chunks of 4
        var items_ptr = items;
        while (items_ptr.len >= 4) {
            self.encodeChunk(items_ptr[0..4], min_doc_id, block_size, true) catch |err| switch (err) {
                error.BlockFull => {
                    // Block is full, stop encoding
                    items_ptr = items_ptr[0..0];
                    break;
                },
            };
            items_ptr = items_ptr[4..];
        }

        // Try to encode remaining items in partial chunk (max 3 items)
        if (items_ptr.len > 0) {
            self.encodeChunk(items_ptr, min_doc_id, block_size, false) catch |err| switch (err) {
                error.BlockFull => {}, // Remaining items will be encoded in next block
            };
        }

        // Flush any remaining unique hashes in temp buffers
        self.flushTempBuffers();

        // Write the block
        self.out_header.first_hash = first_hash;
        self.out_header.counts_offset = @intCast(self.out_hashes.len + self.out_hashes_control.len);
        self.out_header.docids_offset = @intCast(self.out_hashes.len + self.out_hashes_control.len + self.out_counts.len + self.out_counts_control.len);

        var stream = std.io.fixedBufferStream(out);
        var writer = stream.writer();

        // Write header
        var header_bytes: [BLOCK_HEADER_SIZE]u8 = undefined;
        encodeBlockHeader(self.out_header, &header_bytes);
        try writer.writeAll(&header_bytes);

        try writer.writeAll(self.out_hashes_control.slice());
        try writer.writeAll(self.out_hashes.slice());
        try writer.writeAll(self.out_counts_control.slice());
        try writer.writeAll(self.out_counts.slice());
        try writer.writeAll(self.out_docids_control.slice());
        try writer.writeAll(self.out_docids.slice());

        // Zero out remaining space
        const bytes_written = try stream.getPos();
        @memset(out[bytes_written..], 0);

        return self.out_header.num_items;
    }
};

test "encodeBlockHeader/decodeBlockHeader" {
    const header = BlockHeader{
        .first_hash = 12345678,
        .num_hashes = 10,
        .num_items = 25,
        .counts_offset = 20,
        .docids_offset = 30,
    };
    var buffer: [BLOCK_HEADER_SIZE]u8 = undefined;
    encodeBlockHeader(header, buffer[0..]);

    const decoded_header = decodeBlockHeader(buffer[0..]);
    try testing.expectEqual(header.first_hash, decoded_header.first_hash);
    try testing.expectEqual(header.num_hashes, decoded_header.num_hashes);
    try testing.expectEqual(header.num_items, decoded_header.num_items);
    try testing.expectEqual(header.counts_offset, decoded_header.counts_offset);
    try testing.expectEqual(header.docids_offset, decoded_header.docids_offset);
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
    var block: [256]u8 = undefined;
    const consumed = try encoder.encodeBlock(items, min_doc_id, &block);
    try testing.expectEqual(5, consumed);

    const header = decodeBlockHeader(&block);
    try testing.expectEqual(4, header.num_hashes); // Unique hashes: [1,3,4,5] = 4 unique
    try testing.expectEqual(1, header.first_hash);
    try testing.expectEqual(5, header.num_items);

    // Test with BlockReader
    var reader = BlockReader.init(min_doc_id);
    reader.load(&block, false);

    try testing.expectEqual(@as(u16, 5), reader.getNumItems());
    try testing.expectEqual(@as(u32, 1), reader.getFirstHash());

    // Test hash ranges
    const range1 = reader.findHash(1);
    try testing.expectEqual(@as(usize, 0), range1.start);
    try testing.expectEqual(@as(usize, 2), range1.end);

    const range3 = reader.findHash(3);
    try testing.expectEqual(@as(usize, 2), range3.start);
    try testing.expectEqual(@as(usize, 3), range3.end);

    const range4 = reader.findHash(4);
    try testing.expectEqual(@as(usize, 3), range4.start);
    try testing.expectEqual(@as(usize, 4), range4.end);

    const range5 = reader.findHash(5);
    try testing.expectEqual(@as(usize, 4), range5.start);
    try testing.expectEqual(@as(usize, 5), range5.end);

    // Test docids
    const docids1 = reader.getDocidsForRange(range1);
    try testing.expectEqualSlices(u32, &[_]u32{ 100, 200 }, docids1);

    const docids3 = reader.searchHash(3);
    try testing.expectEqualSlices(u32, &[_]u32{300}, docids3);

    const docids4 = reader.searchHash(4);
    try testing.expectEqualSlices(u32, &[_]u32{400}, docids4);

    const docids5 = reader.searchHash(5);
    try testing.expectEqualSlices(u32, &[_]u32{500}, docids5);
}
