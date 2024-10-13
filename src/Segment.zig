const std = @import("std");
const assert = std.debug.assert;

const common = @import("common.zig");
const Item = common.Item;
const SearchResults = common.SearchResults;

const filefmt = @import("filefmt.zig");

allocator: std.mem.Allocator,
version: u32 = 0,
docs: std.AutoHashMap(u32, bool),
index: std.ArrayList(u32),
block_size: usize = 0,
blocks: std.ArrayList(u8),
merged: u32 = 0,

const Self = @This();

pub fn init(allocator: std.mem.Allocator) Self {
    return Self{
        .allocator = allocator,
        .docs = std.AutoHashMap(u32, bool).init(allocator),
        .index = std.ArrayList(u32).init(allocator),
        .blocks = std.ArrayList(u8).init(allocator),
    };
}

pub fn deinit(self: *Self) void {
    self.docs.deinit();
    self.index.deinit();
    self.blocks.deinit();
}

pub fn getBlockData(self: *Self, block: usize) []const u8 {
    return self.blocks.items[block * self.block_size .. (block + 1) * self.block_size];
}

pub fn search(self: *Self, hashes: []const u32, results: *SearchResults) !void {
    assert(std.sort.isSorted(u32, hashes, {}, std.sort.asc(u32)));

    var prev_block_no: usize = std.math.maxInt(usize);
    var prev_block_range_start: usize = 0;

    var block_items = std.ArrayList(Item).init(self.allocator);
    defer block_items.deinit();

    for (hashes) |hash| {
        var block_no = std.sort.lowerBound(u32, hash, self.index.items, {}, std.sort.asc(u32));
        if (block_no == self.index.items.len) {
            block_no = prev_block_range_start;
        }
        prev_block_range_start = block_no;

        while (block_no < self.index.items.len and self.index.items[block_no] <= hash) : (block_no += 1) {
            if (block_no != prev_block_no) {
                prev_block_no = block_no;
                const block_data = self.getBlockData(block_no);
                try filefmt.readBlock(block_data, &block_items);
            }
            const matches = std.sort.equalRange(Item, Item{ .hash = hash, .id = 0 }, block_items.items, {}, Item.cmpByHash);
            for (matches[0]..matches[1]) |i| {
                try results.incr(block_items.items[i].id, self.version);
            }
        }
    }
}
