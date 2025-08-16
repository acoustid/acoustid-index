const std = @import("std");
const assert = std.debug.assert;

const streamvbyte = @import("streamvbyte.zig");
const Item = @import("segment.zig").Item;

// Block handling for compressed (hash, docid) pairs using StreamVByte encoding.
// Each block has a fixed size and are written into a file, they need to be fixed size for easier indexing.
//
// Block format:
//  - u32   min_hash
//  - u32   max_hash
//  - u16   num items
//  - u16   docids offset
//  - []u8  encoded hash deltas (all hashes including duplicates)
//  - []u8  encoded docid deltas

// Block-related constants
pub const MIN_BLOCK_SIZE = 64;
pub const MAX_BLOCK_SIZE = 4096;
pub const MAX_ITEMS_PER_BLOCK = MAX_BLOCK_SIZE / 2;
pub const BLOCK_HEADER_SIZE = 12; // u32 + u32 + u16 + u16

pub const BlockHeader = extern struct {
    min_hash: u32,
    max_hash: u32,
    num_items: u16,
    docids_offset: u16,
};

/// Decode a BlockHeader from bytes
pub fn decodeBlockHeader(data: []const u8) BlockHeader {
    std.debug.assert(data.len >= BLOCK_HEADER_SIZE);
    return std.mem.bytesToValue(BlockHeader, data[0..BLOCK_HEADER_SIZE]);
}

/// Encode a BlockHeader into bytes
pub fn encodeBlockHeader(header: BlockHeader, out_data: []u8) void {
    std.debug.assert(out_data.len >= BLOCK_HEADER_SIZE);
    @memcpy(out_data[0..BLOCK_HEADER_SIZE], std.mem.asBytes(&header));
}

/// BlockReader efficiently searches for hashes within a single compressed block
/// Caches decompressed data to avoid redundant decompression when searching multiple hashes
pub const BlockReader = struct {
    // Configuration
    min_doc_id: u32,

    // Block data (set via load())
    block_data: ?[]const u8 = null,

    // Cached decompressed data
    hashes: [MAX_ITEMS_PER_BLOCK]u32 = undefined,
    docids: [MAX_ITEMS_PER_BLOCK]u32 = undefined,

    // State tracking
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

    /// Get a pointer to the block header from the block data
    fn getHeaderPtr(self: *const BlockReader) *const BlockHeader {
        assert(self.block_data != null);
        assert(self.block_data.?.len >= BLOCK_HEADER_SIZE);
        return @ptrCast(@alignCast(self.block_data.?.ptr));
    }

    /// Load a new block for searching
    /// This resets all cached state
    pub fn load(self: *BlockReader, block_data: []const u8, lazy: bool) void {
        assert(block_data.len >= BLOCK_HEADER_SIZE);

        // Reset all state
        self.block_data = block_data;
        self.hashes_loaded = false;
        self.docids_loaded = false;

        // If full is true, decode all hashes and docids immediately
        if (!lazy) {
            self.ensureHashesLoaded();
            self.ensureDocidsLoaded();
        }
    }

    /// Check if a block is currently loaded
    pub fn isLoaded(self: *const BlockReader) bool {
        return self.block_data != null;
    }

    /// Check if the block is empty (no items)
    pub fn isEmpty(self: *const BlockReader) bool {
        return self.getHeaderPtr().num_items == 0;
    }

    /// Reset the searcher state (clears cached data but keeps block_data)
    pub fn reset(self: *BlockReader) void {
        self.hashes_loaded = false;
        self.docids_loaded = false;
        // Keep block_data as it's still valid
    }

    /// Load and cache all hashes (including duplicates) if not already loaded
    fn ensureHashesLoaded(self: *BlockReader) void {
        if (self.hashes_loaded) return;

        if (self.isEmpty()) {
            self.hashes_loaded = true;
            return;
        }

        const header = self.getHeaderPtr();
        const offset = BLOCK_HEADER_SIZE;
        streamvbyte.decodeValues(
            header.num_items,
            0,
            header.num_items,
            self.block_data.?[offset..],
            &self.hashes,
            .variant0124,
        );
        streamvbyte.svbDeltaDecodeInPlace(self.hashes[0..header.num_items], header.min_hash);
        self.hashes_loaded = true;
    }

    /// Load and cache docids if not already loaded
    fn ensureDocidsLoaded(self: *BlockReader) void {
        if (self.docids_loaded) return;

        if (self.isEmpty()) {
            self.docids_loaded = true;
            return;
        }

        self.ensureHashesLoaded();

        const header = self.getHeaderPtr();
        const offset = BLOCK_HEADER_SIZE + header.docids_offset;
        streamvbyte.decodeValues(
            header.num_items,
            0,
            header.num_items,
            self.block_data.?[offset..],
            &self.docids,
            .variant1234,
        );

        // Apply docid delta decoding with hash boundary resets
        // Each time the hash changes, reset the base to min_doc_id
        var last_docid = self.min_doc_id;
        var last_hash: u32 = if (header.num_items > 0) self.hashes[0] else 0;
        
        for (0..header.num_items) |i| {
            const current_hash = self.hashes[i];
            
            // If hash changed, reset base to min_doc_id
            if (current_hash != last_hash) {
                last_docid = self.min_doc_id;
                last_hash = current_hash;
            }
            
            // Apply delta decoding
            self.docids[i] += last_docid;
            last_docid = self.docids[i];
        }
        
        self.docids_loaded = true;
    }

    /// Get the minimum hash in this block (for block-level filtering)
    pub fn getMinHash(self: *BlockReader) u32 {
        return self.getHeaderPtr().min_hash;
    }

    /// Get the maximum hash in this block (for block-level filtering)
    pub fn getMaxHash(self: *BlockReader) u32 {
        return self.getHeaderPtr().max_hash;
    }

    /// Get the number of items in this block
    pub fn getNumItems(self: *BlockReader) u16 {
        return self.getHeaderPtr().num_items;
    }

    /// Check if this block could contain the given hash
    /// Returns true if hash is within the block's min_hash and max_hash range
    pub fn couldContainHash(self: *const BlockReader, hash: u32) bool {
        const header = self.getHeaderPtr();
        return hash >= header.min_hash and hash <= header.max_hash;
    }

    /// Find all occurrences of a hash in this block
    /// Returns the range [start, end) of matching indices
    pub fn findHash(self: *BlockReader, hash: u32) HashRange {
        if (self.isEmpty()) {
            return HashRange{ .start = 0, .end = 0 };
        }

        self.ensureHashesLoaded();

        const header = self.getHeaderPtr();
        const hashes_slice = self.hashes[0..header.num_items];
        
        // Find all occurrences of hash using equalRange
        const range = std.sort.equalRange(u32, hashes_slice, hash, orderU32);
        
        return HashRange{ .start = range[0], .end = range[1] };
    }

    /// Get docids for a specific range (typically from findHash result)
    /// Caller must ensure range is valid
    pub fn getDocidsForRange(self: *BlockReader, range: HashRange) []const u32 {
        if (range.start >= range.end) {
            return &[_]u32{};
        }

        const header = self.getHeaderPtr();
        // Read StreamVByte-encoded docids for just this range
        const offset = BLOCK_HEADER_SIZE + header.docids_offset;
        _ = streamvbyte.decodeValues(
            header.num_items,
            range.start,
            range.end,
            self.block_data.?[offset..],
            &self.docids,
            .variant1234,
        );

        const docids = self.docids[range.start..range.end];

        // Apply delta decoding - since range is at hash boundaries, 
        // first docid is relative to min_doc_id, rest are relative to previous
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
        return self.hashes[0..self.getHeaderPtr().num_items];
    }

    pub fn getDocids(self: *BlockReader) []const u32 {
        self.ensureDocidsLoaded();
        return self.docids[0..self.getNumItems()];
    }

    /// Get all items as (hash, docid) pairs
    /// Caller must provide a slice with at least getNumItems() capacity
    /// Returns the number of items written to the output slice
    pub fn getItems(self: *BlockReader, output: []Item) usize {
        if (self.isEmpty()) {
            return 0;
        }

        std.debug.assert(output.len >= self.getNumItems());

        self.ensureHashesLoaded();
        self.ensureDocidsLoaded();

        const header = self.getHeaderPtr();

        // Simple 1:1 mapping now
        for (0..header.num_items) |i| {
            output[i] = Item{
                .hash = self.hashes[i],
                .id = self.docids[i],
            };
        }

        return header.num_items;
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
    try testing.expectEqual(@as(u32, 100), reader.getMinHash());
    try testing.expectEqual(@as(u32, 300), reader.getMaxHash());
    try testing.expectEqual(false, reader.isEmpty());

    // Test findHash
    const range100 = reader.findHash(100);
    try testing.expectEqual(BlockReader.HashRange{ .start = 0, .end = 2 }, range100);

    const range200 = reader.findHash(200);
    try testing.expectEqual(BlockReader.HashRange{ .start = 2, .end = 3 }, range200);

    const range404 = reader.findHash(404);
    try testing.expectEqual(BlockReader.HashRange{ .start = 4, .end = 4 }, range404);

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
    out_header: BlockHeader = .{
        .min_hash = 0,
        .max_hash = 0,
        .num_items = 0,
        .docids_offset = 0,
    },

    out_hashes: std.BoundedArray(u8, MAX_BLOCK_SIZE) = .{},
    out_hashes_control: std.BoundedArray(u8, MAX_BLOCK_SIZE) = .{},

    out_docids: std.BoundedArray(u8, MAX_BLOCK_SIZE) = .{},
    out_docids_control: std.BoundedArray(u8, MAX_BLOCK_SIZE) = .{},

    const Self = @This();

    pub fn init() Self {
        return .{};
    }

    pub fn encodeChunk(self: *Self, items: []const Item, min_doc_id: u32, block_size: usize, prev_hash: u32, prev_docid: u32) !struct { last_hash: u32, last_docid: u32 } {
        std.debug.assert(items.len > 0);
        std.debug.assert(items.len <= 4);

        // Prepare hash and docid deltas for this chunk
        var chunk_hashes: [4]u32 = .{ 0, 0, 0, 0 };
        var chunk_docids: [4]u32 = .{ 0, 0, 0, 0 };

        var last_hash = prev_hash;
        var last_docid = prev_docid;

        for (0..items.len) |i| {
            const current_hash = items[i].hash;
            const current_docid = items[i].id;

            // Encode hash delta
            chunk_hashes[i] = current_hash - last_hash;
            
            // Encode docid delta - reset to min_doc_id on hash boundaries
            if (current_hash != last_hash) {
                // Hash changed, encode relative to min_doc_id
                chunk_docids[i] = current_docid - min_doc_id;
                last_docid = current_docid;
            } else {
                // Same hash, encode relative to previous docid
                chunk_docids[i] = current_docid - last_docid;
                last_docid = current_docid;
            }

            last_hash = current_hash;
        }

        // Calculate sizes for this chunk
        const encoded_hashes_size = streamvbyte.svbEncodeQuad0124(
            chunk_hashes,
            self.out_hashes.unusedCapacitySlice(),
            &self.out_hashes_control.buffer[self.out_hashes_control.len],
        );

        const encoded_docids_size = streamvbyte.svbEncodeQuad1234(
            chunk_docids,
            self.out_docids.unusedCapacitySlice(),
            &self.out_docids_control.buffer[self.out_docids_control.len],
        );

        const new_block_size = BLOCK_HEADER_SIZE +
            self.out_hashes.len + encoded_hashes_size + self.out_hashes_control.len + 1 +
            self.out_docids.len + encoded_docids_size + self.out_docids_control.len + 1;

        if (new_block_size > block_size) {
            return error.BlockFull;
        }

        // Commit the chunk
        self.out_hashes.len += encoded_hashes_size;
        self.out_hashes_control.len += 1;
        
        self.out_docids.len += encoded_docids_size;
        self.out_docids_control.len += 1;

        self.out_header.num_items += @intCast(items.len);

        return .{ .last_hash = last_hash, .last_docid = last_docid };
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
        self.out_header.min_hash = first_hash;
        self.out_header.max_hash = first_hash;

        self.out_hashes.clear();
        self.out_hashes_control.clear();
        self.out_docids.clear();
        self.out_docids_control.clear();

        var last_hash = first_hash;
        var last_docid = min_doc_id;

        // Try to encode items in chunks of 4
        var items_ptr = items;
        while (items_ptr.len >= 4) {
            const result = self.encodeChunk(items_ptr[0..4], min_doc_id, block_size, last_hash, last_docid) catch |err| switch (err) {
                error.BlockFull => {
                    // Block is full, stop encoding
                    items_ptr = items_ptr[0..0];
                    break;
                },
            };
            last_hash = result.last_hash;
            last_docid = result.last_docid;
            items_ptr = items_ptr[4..];
        }

        // Try to encode remaining items in partial chunk (max 3 items)
        if (items_ptr.len > 0) {
            _ = self.encodeChunk(items_ptr, min_doc_id, block_size, last_hash, last_docid) catch |err| switch (err) {
                error.BlockFull => {
                    // Remaining items will be encoded in next block
                },
            };
        }

        // Calculate max_hash from the last successfully processed item
        self.out_header.max_hash = if (self.out_header.num_items > 0) items[self.out_header.num_items - 1].hash else first_hash;
        self.out_header.docids_offset = @intCast(self.out_hashes.len + self.out_hashes_control.len);

        var stream = std.io.fixedBufferStream(out);
        var writer = stream.writer();

        try writer.writeAll(std.mem.asBytes(&self.out_header));
        try writer.writeAll(self.out_hashes_control.slice());
        try writer.writeAll(self.out_hashes.slice());
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
        .min_hash = 12345678,
        .max_hash = 87654321,
        .num_items = 25,
        .docids_offset = 30,
    };
    var buffer: [BLOCK_HEADER_SIZE]u8 = undefined;
    encodeBlockHeader(header, buffer[0..]);

    const decoded_header = decodeBlockHeader(buffer[0..]);
    try testing.expectEqual(header.min_hash, decoded_header.min_hash);
    try testing.expectEqual(header.max_hash, decoded_header.max_hash);
    try testing.expectEqual(header.num_items, decoded_header.num_items);
    try testing.expectEqual(header.docids_offset, decoded_header.docids_offset);
}

test "BlockEncoder with mixed hashes and docids" {
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
    try testing.expectEqual(1, header.min_hash);
    try testing.expectEqual(5, header.max_hash);
    try testing.expectEqual(5, header.num_items);

    // Test with BlockReader
    var reader = BlockReader.init(min_doc_id);
    reader.load(&block, false);

    try testing.expectEqual(@as(u16, 5), reader.getNumItems());
    try testing.expectEqual(@as(u32, 1), reader.getMinHash());
    try testing.expectEqual(@as(u32, 5), reader.getMaxHash());

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

test "BlockEncoder with duplicate hashes" {
    var encoder = BlockEncoder.init();

    // Test case: multiple items with the same hash to verify correct encoding
    const items: []const Item = &.{
        .{ .hash = 100, .id = 1 }, // First item should be encoded as (1 - 1) = 0
        .{ .hash = 100, .id = 2 }, // Same hash, should be encoded as (2 - 1) = 1
        .{ .hash = 100, .id = 3 }, // Same hash, should be encoded as (3 - 2) = 1
    };

    const min_doc_id: u32 = 1;
    var block: [256]u8 = undefined;
    const consumed = try encoder.encodeBlock(items, min_doc_id, &block);
    try testing.expectEqual(3, consumed);

    // Verify that the encoding produced the correct header
    const header = decodeBlockHeader(&block);
    try testing.expectEqual(@as(u32, 100), header.min_hash);
    try testing.expectEqual(@as(u32, 100), header.max_hash);
    try testing.expectEqual(@as(u16, 3), header.num_items);

    // Test that decoding produces the original docids
    var reader = BlockReader.init(min_doc_id);
    reader.load(&block, false);

    const docids = reader.searchHash(100);
    try testing.expectEqualSlices(u32, &[_]u32{ 1, 2, 3 }, docids);

    // Alternative test using getItems
    var items_output: [3]Item = undefined;
    const num_items = reader.getItems(&items_output);
    try testing.expectEqual(@as(usize, 3), num_items);

    // All items should have hash 100 and docids 1, 2, 3
    for (items_output, 0..) |item, i| {
        try testing.expectEqual(@as(u32, 100), item.hash);
        try testing.expectEqual(@as(u32, @intCast(i + 1)), item.id);
    }
}

test "BlockEncoder reuse across multiple blocks" {
    var encoder = BlockEncoder.init();

    // First block: items with hash=100
    const items1: []const Item = &.{
        .{ .hash = 100, .id = 1 },
        .{ .hash = 100, .id = 2 },
    };

    const min_doc_id: u32 = 1;
    var block1: [256]u8 = undefined;
    const consumed1 = try encoder.encodeBlock(items1, min_doc_id, &block1);
    try testing.expectEqual(2, consumed1);

    // Verify first block is correct
    var reader1 = BlockReader.init(min_doc_id);
    reader1.load(&block1, false);
    const docids1 = reader1.searchHash(100);
    try testing.expectEqualSlices(u32, &[_]u32{ 1, 2 }, docids1);

    // Second block: more items with hash=100 (same hash as previous block)
    // This tests that encoder properly handles state across multiple blocks
    const items2: []const Item = &.{
        .{ .hash = 100, .id = 3 }, // This should be encoded as (3 - 1) = 2, but...
        .{ .hash = 100, .id = 4 }, // This should be encoded as (4 - 3) = 1
    };

    var block2: [256]u8 = undefined;
    const consumed2 = try encoder.encodeBlock(items2, min_doc_id, &block2);
    try testing.expectEqual(2, consumed2);

    // Verify that encoder correctly handles multiple blocks
    var reader2 = BlockReader.init(min_doc_id);
    reader2.load(&block2, false);
    const docids2 = reader2.searchHash(100);

    // Should correctly encode and decode the second block
    try testing.expectEqualSlices(u32, &[_]u32{ 3, 4 }, docids2);
}

test "BlockReader.couldContainHash" {
    // Create a test block with known min/max hash range
    var encoder = BlockEncoder.init();
    const items = [_]Item{
        .{ .hash = 100, .id = 1 },
        .{ .hash = 200, .id = 2 },
        .{ .hash = 300, .id = 3 },
    };

    const min_doc_id: u32 = 1;
    var block_data: [256]u8 = undefined;
    _ = try encoder.encodeBlock(&items, min_doc_id, &block_data);

    var reader = BlockReader.init(min_doc_id);
    reader.load(&block_data, false);

    // Test hash range checking
    try testing.expect(reader.couldContainHash(100)); // min_hash
    try testing.expect(reader.couldContainHash(200)); // middle
    try testing.expect(reader.couldContainHash(300)); // max_hash

    // Test edge cases
    try testing.expect(!reader.couldContainHash(99)); // just below min_hash
    try testing.expect(!reader.couldContainHash(301)); // just above max_hash

    // Test definitely out of range
    try testing.expect(!reader.couldContainHash(50)); // way below
    try testing.expect(!reader.couldContainHash(500)); // way above
}
