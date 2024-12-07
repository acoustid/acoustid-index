const std = @import("std");
const log = std.log;

const common = @import("common.zig");
const SearchResults = common.SearchResults;
const KeepOrDelete = common.KeepOrDelete;
const SegmentInfo = @import("segment.zig").SegmentInfo;
const Item = @import("segment.zig").Item;

const Change = @import("change.zig").Change;

const Deadline = @import("utils/Deadline.zig");

const SegmentMerger = @import("segment_merger.zig").SegmentMerger;

const Self = @This();

pub const Options = struct {};

allocator: std.mem.Allocator,
info: SegmentInfo = .{},
attributes: std.StringHashMapUnmanaged(u64) = .{},
docs: std.AutoHashMapUnmanaged(u32, bool) = .{},
min_doc_id: u32 = 0,
max_doc_id: u32 = 0,
items: std.ArrayListUnmanaged(Item) = .{},
frozen: bool = false,

pub fn init(allocator: std.mem.Allocator, opts: Options) Self {
    _ = opts;
    return .{
        .allocator = allocator,
    };
}

pub fn deinit(self: *Self, delete_file: KeepOrDelete) void {
    _ = delete_file;

    var iter = self.attributes.iterator();
    while (iter.next()) |e| {
        self.allocator.free(e.key_ptr.*);
    }
    self.attributes.deinit(self.allocator);
    self.docs.deinit(self.allocator);
    self.items.deinit(self.allocator);
}

pub fn search(self: Self, sorted_hashes: []const u32, results: *SearchResults) !void {
    var items = self.items.items;
    for (sorted_hashes) |hash| {
        const matches = std.sort.equalRange(Item, Item{ .hash = hash, .id = 0 }, items, {}, Item.cmpByHash);
        for (matches[0]..matches[1]) |i| {
            try results.incr(items[i].id, self.info.version);
        }
        items = items[matches[1]..];
    }
}

pub fn getSize(self: Self) usize {
    return self.items.items.len;
}

pub fn build(self: *Self, changes: []const Change) !void {
    var num_attributes: u32 = 0;
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
            .set_attribute => {
                num_attributes += 1;
            },
        }
    }

    try self.attributes.ensureTotalCapacity(self.allocator, num_attributes);
    try self.docs.ensureTotalCapacity(self.allocator, num_docs);
    try self.items.ensureTotalCapacity(self.allocator, num_items);

    self.min_doc_id = 0;
    self.max_doc_id = 0;
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
                    if (self.min_doc_id == 0 or op.id < self.min_doc_id) {
                        self.min_doc_id = op.id;
                    }
                    if (self.max_doc_id == 0 or op.id > self.max_doc_id) {
                        self.max_doc_id = op.id;
                    }
                }
            },
            .delete => |op| {
                const result = self.docs.getOrPutAssumeCapacity(op.id);
                if (!result.found_existing) {
                    result.value_ptr.* = false;
                    if (self.min_doc_id == 0 or op.id < self.min_doc_id) {
                        self.min_doc_id = op.id;
                    }
                    if (self.max_doc_id == 0 or op.id > self.max_doc_id) {
                        self.max_doc_id = op.id;
                    }
                }
            },
            .set_attribute => |op| {
                const result = self.attributes.getOrPutAssumeCapacity(op.name);
                if (!result.found_existing) {
                    errdefer self.attributes.removeByPtr(result.key_ptr);
                    result.key_ptr.* = try self.allocator.dupe(u8, op.name);
                    result.value_ptr.* = op.value;
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
    std.debug.assert(self.allocator.ptr == merger.allocator.ptr);

    self.info = merger.segment.info;

    self.attributes.deinit(self.allocator);
    self.attributes = merger.segment.attributes.move();

    self.docs.deinit(self.allocator);
    self.docs = merger.segment.docs.move();

    self.max_doc_id = merger.segment.max_doc_id;

    self.items.clearRetainingCapacity();
    try self.items.ensureTotalCapacity(self.allocator, merger.estimated_size);
    while (true) {
        const item = try merger.read() orelse break;
        try self.items.append(self.allocator, item);
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
