const std = @import("std");
const assert = std.debug.assert;

const streamvbyte = @import("streamvbyte.zig");

/// BlockReader efficiently searches for hashes within a single compressed block
/// Caches decompressed data to avoid redundant decompression when searching multiple hashes
pub const BlockReader = struct {
    // Configuration
    min_doc_id: u32,

    // Block data (set via load())
    block_data: ?[]const u8 = null,

    // Cached decompressed data
    block_header: streamvbyte.BlockHeader = .{ .num_items = 0, .docid_list_offset = 0, .first_hash = 0 },
    hashes: [streamvbyte.MAX_ITEMS_PER_BLOCK]u32 = undefined,
    docids: [streamvbyte.MAX_ITEMS_PER_BLOCK]u32 = undefined,

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
        assert(block_data.len >= streamvbyte.BLOCK_HEADER_SIZE);

        // Reset all state
        self.block_data = block_data;
        self.header_loaded = false;
        self.hashes_loaded = false;
        self.docids_loaded = false;

        // Load header immediately
        self.block_header = streamvbyte.decodeBlockHeader(block_data);
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

        const num_hashes = streamvbyte.decodeBlockHashes(self.block_header, self.block_data.?, &self.hashes);
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

        const num_docids = streamvbyte.decodeBlockDocids(self.block_header, self.hashes[0..self.block_header.num_items], self.block_data.?, self.min_doc_id, &self.docids);
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

        self.ensureDocidsLoaded();
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
    var encoder = streamvbyte.BlockEncoder.init();
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
