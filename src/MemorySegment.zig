const std = @import("std");
const log = std.log;

const common = @import("common.zig");
const Item = common.Item;
const SearchResults = common.SearchResults;
const SegmentId = common.SegmentId;

const Change = @import("change.zig").Change;

const Deadline = @import("utils/Deadline.zig");

const SegmentMerger = @import("segment_merger.zig").SegmentMerger;

const Self = @This();

allocator: std.mem.Allocator,
id: SegmentId = .{ .version = 0, .included_merges = 0 },
max_commit_id: u64 = 0,
docs: std.AutoHashMap(u32, bool),
items: std.ArrayList(Item),
frozen: bool = false,

pub fn init(allocator: std.mem.Allocator) Self {
    return .{
        .allocator = allocator,
        .docs = std.AutoHashMap(u32, bool).init(allocator),
        .items = std.ArrayList(Item).init(allocator),
    };
}

pub fn deinit(self: *Self) void {
    self.docs.deinit();
    self.items.deinit();
}

pub fn search(self: Self, sorted_hashes: []const u32, results: *SearchResults) !void {
    var items = self.items.items;
    for (sorted_hashes) |hash| {
        const matches = std.sort.equalRange(Item, Item{ .hash = hash, .id = 0 }, items, {}, Item.cmpByHash);
        for (matches[0]..matches[1]) |i| {
            try results.incr(items[i].id, self.id.version);
        }
        items = items[matches[1]..];
    }
}

pub fn isFrozen(self: Self) bool {
    return self.frozen;
}

pub fn canBeMerged(self: Self) bool {
    return !self.frozen;
}

pub fn getSize(self: Self) usize {
    return self.items.items.len;
}

pub fn build(self: *Self, changes: []const Change) !void {
    var num_docs: u32 = 0;
    var num_items: usize = 0;
    for (changes) |change| {
        switch (change) {
            .insert => |op| {
                num_docs += 1;
                num_items += op.hashes.len;
            },
            .delete => {
                num_docs += 1;
            },
        }
    }

    try self.docs.ensureTotalCapacity(num_docs);
    try self.items.ensureTotalCapacity(num_items);

    var i = changes.len;
    while (i > 0) {
        i -= 1;
        const change = changes[i];
        switch (change) {
            .insert => |op| {
                const result = self.docs.getOrPutAssumeCapacity(op.id);
                if (!result.found_existing) {
                    result.value_ptr.* = true;
                    var items = self.items.addManyAsSliceAssumeCapacity(op.hashes.len);
                    for (op.hashes, 0..) |hash, j| {
                        items[j] = .{ .hash = hash, .id = op.id };
                    }
                }
            },
            .delete => |op| {
                const result = self.docs.getOrPutAssumeCapacity(op.id);
                if (!result.found_existing) {
                    result.value_ptr.* = false;
                }
            },
        }
    }

    std.sort.pdq(Item, self.items.items, {}, Item.cmp);
}

pub fn cleanup(self: *Self) void {
    _ = self;
}

pub fn merge(self: *Self, merger: *SegmentMerger(Self)) !void {
    self.id = merger.segment.id;
    self.max_commit_id = merger.segment.max_commit_id;

    self.docs.deinit();
    self.docs = merger.segment.docs.move();

    self.items.clearRetainingCapacity();
    try self.items.ensureTotalCapacity(merger.estimated_size);
    while (true) {
        const item = try merger.read() orelse break;
        try self.items.append(item);
        merger.advance();
    }
}

pub fn reader(self: *const Self) Reader {
    return .{
        .segment = self,
        .index = 0,
    };
}

pub const Reader = struct {
    segment: *const Self,
    index: usize,

    pub fn close(self: *Reader) void {
        _ = self;
    }

    pub fn read(self: *Reader) !?Item {
        if (self.index < self.segment.items.items.len) {
            return self.segment.items.items[self.index];
        } else {
            return null;
        }
    }

    pub fn advance(self: *Reader) void {
        if (self.index < self.segment.items.items.len) {
            self.index += 1;
        }
    }
};
