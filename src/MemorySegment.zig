const std = @import("std");
const zio = @import("zio");
const log = std.log;

const common = @import("common.zig");
const SearchResults = common.SearchResults;
const SegmentInfo = @import("segment.zig").SegmentInfo;
const SegmentStatus = @import("segment.zig").SegmentStatus;
const Item = @import("segment.zig").Item;

const Change = @import("change.zig").Change;
const Metadata = @import("Metadata.zig");

// NOTE: merge()/reader()/Reader and the SegmentMerger dependency are deferred to
// the merging step; this port covers build + search for the in-memory path.

const Self = @This();

pub const Options = struct {};

allocator: std.mem.Allocator,
info: SegmentInfo = .{},
status: SegmentStatus = .{},
metadata: Metadata,
docs: std.AutoHashMapUnmanaged(u32, bool) = .{},
min_doc_id: u32 = 0,
max_doc_id: u32 = 0,
items: std.ArrayListUnmanaged(Item) = .empty,

pub fn init(allocator: std.mem.Allocator, opts: Options) Self {
    _ = opts;
    return .{
        .allocator = allocator,
        .metadata = Metadata.initOwned(allocator),
    };
}

pub fn deinit(self: *Self) void {
    self.metadata.deinit();
    self.docs.deinit(self.allocator);
    self.items.deinit(self.allocator);
}

pub fn search(self: Self, sorted_hashes: []const u32, results: *SearchResults) !void {
    var items = self.items.items;
    for (sorted_hashes) |hash| {
        try zio.maybeYield();
        const matches = std.sort.equalRange(Item, items, Item{ .hash = hash, .id = 0 }, Item.orderByHash);
        for (matches[0]..matches[1]) |i| {
            try results.incr(items[i].id, self.info.commit_id);
        }
        items = items[matches[1]..];
    }
}

pub fn getSize(self: Self) usize {
    return self.items.items.len;
}

// Build this memory segment from a SegmentMerger's output (used for in-memory
// merges). `merger.segment` holds the merged info/metadata/docs; read() yields
// the merged items in order.
pub fn buildFromMerger(self: *Self, merger: anytype) !void {
    self.info = merger.segment.info;
    self.min_doc_id = merger.segment.min_doc_id;
    self.max_doc_id = merger.segment.max_doc_id;

    try self.metadata.update(merger.segment.metadata);

    try self.docs.ensureTotalCapacity(self.allocator, merger.segment.docs.count());
    var it = merger.segment.docs.iterator();
    while (it.next()) |entry| self.docs.putAssumeCapacity(entry.key_ptr.*, entry.value_ptr.*);

    try self.items.ensureTotalCapacity(self.allocator, merger.estimated_size);
    while (try merger.read()) |item| {
        try self.items.append(self.allocator, item);
        merger.advance();
    }
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
            .set_metadata => {},
        }
    }

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
            .set_metadata => {}, // applied in a forward pass below (last wins)
        }
    }

    std.sort.pdq(Item, self.items.items, {}, Item.lessThan);

    // A fingerprint is a set of hashes: retain one posting for each (hash, id)
    // pair. Besides preserving set-based scoring, this keeps duplicate input
    // hashes from inflating a fingerprint's score.
    var out: usize = 0;
    for (self.items.items) |item| {
        if (out == 0 or Item.order(self.items.items[out - 1], item) != .eq) {
            self.items.items[out] = item;
            out += 1;
        }
    }
    self.items.items.len = out;

    // Metadata is order-sensitive (last write wins), so apply it forward — unlike
    // the doc loop above, which runs in reverse for first-occurrence-wins on ids.
    for (changes) |change| {
        if (change == .set_metadata) {
            for (change.set_metadata.entries) |e| try self.metadata.set(e.key, e.value);
        }
    }
}

pub fn cleanup(self: *Self) void {
    _ = self;
}

pub fn reader(self: *const Self) Reader {
    return .{ .segment = self, .index = 0 };
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
        self.index += 1;
    }
};
