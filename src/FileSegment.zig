const std = @import("std");
const log = std.log.scoped(.segment);
const assert = std.debug.assert;

const common = @import("common.zig");
const SearchResults = common.SearchResults;
const KeepOrDelete = common.KeepOrDelete;
const Deadline = @import("utils/Deadline.zig");

const Item = @import("segment.zig").Item;
const SegmentInfo = @import("segment.zig").SegmentInfo;
const SegmentStatus = @import("segment.zig").SegmentStatus;
const metrics = @import("metrics.zig");

const filefmt = @import("filefmt.zig");
const streamvbyte = @import("streamvbyte.zig");
const BlockReader = @import("block.zig").BlockReader;
const MAX_ITEMS_PER_BLOCK = @import("block.zig").MAX_ITEMS_PER_BLOCK;

const Self = @This();

pub const Options = struct {
    dir: std.fs.Dir,
};

allocator: std.mem.Allocator,
dir: std.fs.Dir,
info: SegmentInfo = .{},
status: SegmentStatus = .{},
attributes: std.StringHashMapUnmanaged(u64) = .{},
docs: std.AutoHashMapUnmanaged(u32, bool) = .{},
min_doc_id: u32 = 0,
max_doc_id: u32 = 0,
index: std.ArrayListUnmanaged(u32) = .{},
block_size: usize = 0,
blocks: []const u8,
merged: u32 = 0,
num_items: usize = 0,
num_blocks: usize = 0,
delete_in_deinit: bool = false,

mmaped_file: ?std.fs.File = null,
mmaped_data: ?[]align(std.heap.page_size_min) u8 = null,

pub fn init(allocator: std.mem.Allocator, options: Options) Self {
    return Self{
        .allocator = allocator,
        .dir = options.dir,
        .blocks = undefined,
    };
}

pub fn deinit(self: *Self, delete_file: KeepOrDelete) void {
    var iter = self.attributes.iterator();
    while (iter.next()) |e| {
        self.allocator.free(e.key_ptr.*);
    }
    self.attributes.deinit(self.allocator);
    self.docs.deinit(self.allocator);
    self.index.deinit(self.allocator);

    if (self.mmaped_data) |data| {
        std.posix.munmap(data);
    }

    if (self.mmaped_file) |file| {
        file.close();
    }

    if (delete_file == .delete) {
        self.delete();
    }
}

pub fn loadBlockData(self: Self, block_no: usize, block_reader: *BlockReader, lazy: bool) void {
    std.debug.assert(block_no < self.num_blocks);
    const start = block_no * self.block_size;
    const end = (block_no + 1) * self.block_size;
    // Add extra SIMD padding for safe decoding - ensure we don't exceed blocks bounds
    const padded_end = @min(end + streamvbyte.SIMD_DECODE_PADDING, self.blocks.len);
    const block_data = self.blocks[start..padded_end];
    block_reader.load(block_data, lazy);
}

fn compareHashes(a: u32, b: u32) std.math.Order {
    return std.math.order(a, b);
}

// Maximum blocks to scan per hash - matches cache size for optimal reuse
const MAX_BLOCKS_PER_HASH = 4;

// Maximum documents per hash before early exit to avoid excessive processing
const MAX_DOCS_PER_HASH = 1000;

const BlockCacheEntry = struct {
    block_no: usize,
    block_reader: BlockReader,
};

pub fn search(self: Self, sorted_hashes: []const u32, results: *SearchResults, deadline: Deadline) !void {
    var prev_block_range_start: usize = 0;

    // Initialize block cache with CACHE_SIZE BlockReaders
    var block_cache = [_]BlockCacheEntry{BlockCacheEntry{
        .block_no = std.math.maxInt(usize),
        .block_reader = BlockReader.init(self.min_doc_id),
    }} ** MAX_BLOCKS_PER_HASH;

    // Let's say we have blocks like this:
    //
    // |4.......|6.......|9.......|
    //
    // We want to find hash=2, lowerBound returns block=0 (4), so we start at that block.
    // We want to find hash=6, lowerBound returns block=1 (6), but block=0 could still contain hash=6, so we go one back.
    // We want to find hash=7, lowerBound returns block=2 (9), but block=1 could still contain hash=6, so we go one back.
    // We want to find hash=10, lowerBound returns block=3 (EOF), but block=2 could still contain hash=6, so we go one back.

    for (sorted_hashes, 1..) |hash, i| {
        var block_no = std.sort.lowerBound(u32, self.index.items[prev_block_range_start..], hash, compareHashes) + prev_block_range_start;
        if (block_no > 0) {
            block_no -= 1;
        }
        prev_block_range_start = block_no;

        var num_docs: usize = 0;
        var num_blocks: u64 = 0;
        
        while (block_no < self.index.items.len and self.index.items[block_no] <= hash) : (block_no += 1) {
            // Use block_no % MAX_BLOCKS_PER_HASH as cache key
            const cache_key = block_no % MAX_BLOCKS_PER_HASH;
            var block_reader: *BlockReader = undefined;
            
            if (block_cache[cache_key].block_no == block_no) {
                // Cache hit - reuse existing block_reader
                block_reader = &block_cache[cache_key].block_reader;
            } else {
                // Cache miss - load block data into cache slot
                block_cache[cache_key].block_no = block_no;
                block_reader = &block_cache[cache_key].block_reader;
                self.loadBlockData(block_no, block_reader, true);
            }

            // Search for hash matches and get docids
            const matched_docids = block_reader.searchHash(hash);
            for (matched_docids) |docid| {
                try results.incr(docid, self.info.version);
            }

            num_docs += matched_docids.len;
            if (num_docs > MAX_DOCS_PER_HASH) {
                break; // Early exit to avoid excessive processing for high-frequency hashes
            }
            num_blocks += 1;
            if (num_blocks >= MAX_BLOCKS_PER_HASH) {
                break; // Limit the number of scanned blocks per hash
            }
        }

        metrics.scannedDocsPerHash(num_docs);
        metrics.scannedBlocksPerHash(num_blocks);

        if (i % 10 == 0) {
            try deadline.check();
        }
    }
}

pub fn load(self: *Self, info: SegmentInfo) !void {
    try filefmt.readSegmentFile(self.dir, info, self);
}

pub fn delete(self: *Self) void {
    var file_name_buf: [filefmt.max_file_name_size]u8 = undefined;
    const file_name = filefmt.buildSegmentFileName(&file_name_buf, self.info);

    log.info("deleting segment file {s}", .{file_name});

    self.dir.deleteFile(file_name) catch |err| {
        if (err != error.FileNotFound) {
            log.err("failed to clean up segment file {s}: {}", .{ file_name, err });
        }
    };
}

pub fn cleanup(self: *Self) void {
    self.delete();
}

pub fn merge(self: *Self, source: anytype) !void {
    try self.build(source);
}

pub fn build(self: *Self, source: anytype) !void {
    var file_name_buf: [filefmt.max_file_name_size]u8 = undefined;
    const file_name = filefmt.buildSegmentFileName(&file_name_buf, source.segment.info);

    try filefmt.writeSegmentFile(self.dir, source);

    errdefer self.dir.deleteFile(file_name) catch |err| {
        if (err != error.FileNotFound) {
            log.err("failed to clean up segment file {s}: {}", .{ file_name, err });
        }
    };

    try filefmt.readSegmentFile(self.dir, source.segment.info, self);
}

test "build and reader with duplicate hashes" {
    const MemorySegment = @import("MemorySegment.zig");
    const Change = @import("change.zig").Change;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var source = MemorySegment.init(std.testing.allocator, .{});
    defer source.deinit(.delete);

    source.info = .{ .version = 1 };
    source.status.frozen = true;
    
    // Create data where the same hash appears multiple times (multiple documents with same hash)
    // This creates a scenario where num_hashes < num_items in the hybrid format
    // Use proper Change API to add fingerprints
    const changes = [_]Change{
        .{ .insert = .{ .id = 1, .hashes = &[_]u32{100} } },
        .{ .insert = .{ .id = 2, .hashes = &[_]u32{100} } },
        .{ .insert = .{ .id = 3, .hashes = &[_]u32{100} } },
    };
    
    try source.build(&changes);

    var source_reader = source.reader();
    defer source_reader.close();

    var segment = Self.init(std.testing.allocator, .{ .dir = tmp_dir.dir });
    defer segment.deinit(.delete);

    try segment.build(&source_reader);

    // Verify segment metadata (from old build test)
    try std.testing.expectEqualDeep(SegmentInfo{ .version = 1, .merges = 0 }, segment.info);
    try std.testing.expectEqual(3, segment.docs.count()); // 3 documents inserted
    try std.testing.expectEqual(1, segment.index.items.len); // 1 block for this small dataset

    // Collect all items from the FileSegment reader
    var file_reader = segment.reader();
    defer file_reader.close();
    
    var actual_items = std.ArrayList(Item).init(std.testing.allocator);
    defer actual_items.deinit();
    
    while (try file_reader.read()) |item| {
        try actual_items.append(item);
        file_reader.advance();
    }
    
    // Expected items (same as what we inserted)
    const expected_items = [_]Item{
        .{ .hash = 100, .id = 1 },
        .{ .hash = 100, .id = 2 },
        .{ .hash = 100, .id = 3 },
    };
    
    // Compare the slices directly
    try std.testing.expectEqualSlices(Item, &expected_items, actual_items.items);
}

pub fn getSize(self: Self) usize {
    return self.num_items;
}

pub fn reader(self: *const Self) Reader {
    return .{
        .segment = self,
        .block_reader = BlockReader.init(self.min_doc_id),
    };
}

pub const Reader = struct {
    segment: *const Self,
    index: usize = 0,
    block_no: usize = 0,
    block_reader: BlockReader,
    current_items: [MAX_ITEMS_PER_BLOCK]Item = undefined,
    current_items_len: usize = 0,

    pub fn close(_: *Reader) void {}

    pub fn read(self: *Reader) !?Item {
        while (self.index >= self.current_items_len) {
            if (self.block_no >= self.segment.index.items.len) {
                return null;
            }
            self.index = 0;
            self.segment.loadBlockData(self.block_no, &self.block_reader, false);
            self.current_items_len = self.block_reader.getItems(&self.current_items);
            self.block_no += 1;
        }
        return self.current_items[self.index];
    }

    pub fn advance(self: *Reader) void {
        if (self.index < self.current_items_len) {
            self.index += 1;
        }
    }
};
