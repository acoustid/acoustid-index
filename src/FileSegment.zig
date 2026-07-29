// An immutable on-disk segment, loaded whole into an aligned heap buffer.
// (mlock'd anonymous memory replaces the heap buffer later.) All the block and
// block-index slices point into `data`. Search binary-searches the block index
// per query hash, then decodes only the candidate blocks.

const metrics = @import("metrics.zig");
const std = @import("std");
const zio = @import("zio");
const log = std.log.scoped(.file_segment);
const assert = std.debug.assert;

const common = @import("common.zig");
const SearchResults = common.SearchResults;
const SegmentInfo = @import("segment.zig").SegmentInfo;
const Metadata = @import("Metadata.zig");

const filefmt = @import("filefmt.zig");
const streamvbyte = @import("streamvbyte.zig");
const block = @import("block.zig");
const BlockReader = block.BlockReader;

const Self = @This();

// Match the block cache to the per-hash scan limit for good reuse.
const MAX_BLOCKS_PER_HASH = 4;
const MAX_DOCS_PER_HASH = 1000;

const BlockCacheEntry = struct {
    block_no: usize,
    block_reader: BlockReader,
};

allocator: std.mem.Allocator,
// Directory the segment file lives in; used to delete the file on deinit when
// delete_on_destroy is set.
dir: zio.Dir = undefined,
info: SegmentInfo = .{},
metadata: Metadata,
docs: std.AutoHashMapUnmanaged(u32, bool) = .{},
min_doc_id: u32 = 0,
max_doc_id: u32 = 0,
block_size: usize = 0,
blocks: []const u8 = &.{},
block_index: []const u32 = &.{},
num_items: usize = 0,
num_blocks: usize = 0,
// Owned aligned buffer holding the whole file; blocks/block_index slice into it.
data: []align(64) u8 = &.{},
// Set true (once, at retirement) when a merge supersedes this segment. Its backing
// file is then deleted when the last reference drops — an intrinsic property of the
// retired segment, not a decision made at each release site. Live segments are never
// marked, so shutdown keeps their files.
delete_on_destroy: bool = false,

pub fn init(allocator: std.mem.Allocator) Self {
    return .{ .allocator = allocator, .metadata = Metadata.initOwned(allocator) };
}

// Runs when the last reference is released (so an in-flight reader keeps the file
// until done). Deletes the backing file iff this segment was retired by a merge.
// Only loaded segments (with a data buffer, hence a valid `dir`) have a file.
pub fn deinit(self: *Self) void {
    if (self.delete_on_destroy and self.data.len > 0) {
        var buf: [64]u8 = undefined;
        const name = std.fmt.bufPrint(&buf, "{x:0>16}-{x:0>8}.data", .{ self.info.commit_id, self.info.merges }) catch unreachable;
        self.dir.deleteFile(name) catch |err| {
            if (err != error.FileNotFound) log.warn("failed to delete segment file {s}: {}", .{ name, err });
        };
    }
    self.metadata.deinit();
    self.docs.deinit(self.allocator);
    if (self.data.len > 0) self.allocator.free(self.data);
}

pub fn getSize(self: Self) usize {
    return self.num_items;
}

fn compareHashes(a: u32, b: u32) std.math.Order {
    return std.math.order(a, b);
}

fn loadBlockData(self: Self, block_no: usize, block_reader: *BlockReader, lazy: bool) void {
    assert(block_no < self.num_blocks);
    const start = block_no * self.block_size;
    const end = (block_no + 1) * self.block_size;
    const padded_end = @min(end + streamvbyte.SIMD_DECODE_PADDING, self.blocks.len);
    block_reader.load(self.blocks[start..padded_end], lazy);
}

const Item = @import("segment.zig").Item;

pub fn reader(self: *const Self) Reader {
    return .{ .segment = self, .block_reader = BlockReader.init(self.min_doc_id) };
}

// Yields all items in sorted (hash, id) order by walking blocks. Used by the
// merger to build a new (merged) segment.
pub const Reader = struct {
    segment: *const Self,
    block_no: usize = 0,
    index_in_block: usize = 0,
    block_reader: BlockReader,
    block_loaded: bool = false,

    pub fn close(self: *Reader) void {
        _ = self;
    }

    pub fn read(self: *Reader) !?Item {
        while (true) {
            if (self.block_no >= self.segment.num_blocks) return null;
            if (!self.block_loaded) {
                self.segment.loadBlockData(self.block_no, &self.block_reader, false);
                self.block_loaded = true;
                self.index_in_block = 0;
            }
            if (self.index_in_block >= self.block_reader.getNumItems()) {
                self.block_no += 1;
                self.block_loaded = false;
                continue;
            }
            return Item{
                .hash = self.block_reader.hashes[self.index_in_block],
                .id = self.block_reader.docids[self.index_in_block],
            };
        }
    }

    pub fn advance(self: *Reader) void {
        self.index_in_block += 1;
    }
};

pub fn search(self: Self, sorted_hashes: []const u32, results: *SearchResults) !void {
    var prev_block_range_start: usize = 0;

    var block_cache = [_]BlockCacheEntry{.{
        .block_no = std.math.maxInt(usize),
        .block_reader = BlockReader.init(self.min_doc_id),
    }} ** MAX_BLOCKS_PER_HASH;

    for (sorted_hashes) |hash| {
        try zio.maybeYield();
        var block_no = prev_block_range_start + std.sort.lowerBound(
            u32,
            self.block_index[prev_block_range_start..],
            hash,
            compareHashes,
        );
        prev_block_range_start = block_no;

        var num_docs: usize = 0;
        var num_blocks: u64 = 0;

        while (block_no < self.block_index.len) : (block_no += 1) {
            const cache_entry = &block_cache[block_no % MAX_BLOCKS_PER_HASH];
            if (cache_entry.block_no != block_no) {
                cache_entry.block_no = block_no;
                self.loadBlockData(block_no, &cache_entry.block_reader, true);
            }
            const block_reader = &cache_entry.block_reader;

            if (block_reader.getMinHash() > hash) break;

            const matched = block_reader.searchHash(hash);
            for (matched) |docid| {
                try results.incr(docid, self.info.commit_id);
            }

            num_blocks += 1;
            num_docs += matched.len;
            if (num_blocks >= MAX_BLOCKS_PER_HASH) break;
            if (num_docs > MAX_DOCS_PER_HASH) break;
        }

        metrics.observeScannedDocsPerHash(num_docs);
        metrics.observeScannedBlocksPerHash(num_blocks);
    }
}
