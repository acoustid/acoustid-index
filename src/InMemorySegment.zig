const std = @import("std");
const log = std.log;
const io = std.io;
const assert = std.debug.assert;

const common = @import("common.zig");
const Item = common.Item;
const SearchResults = common.SearchResults;
const SegmentID = common.SegmentID;

const Deadline = @import("utils/Deadline.zig");

const filefmt = @import("filefmt.zig");

const segment_list = @import("segment_list.zig");
pub const List = segment_list.SegmentList(Self);

const Self = @This();

allocator: std.mem.Allocator,
id: SegmentID = .{ .version = 0, .included_merges = 0 },
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

pub fn write(self: *Self, writer: anytype) !void {
    if (self.id == 0) {
        return error.InvalidSegmentVersion;
    }
    try filefmt.writeFile(writer, self);
}

pub fn search(self: *Self, hashes: []const u32, results: *SearchResults) !void {
    assert(std.sort.isSorted(u32, hashes, {}, std.sort.asc(u32)));
    var items = self.items.items;
    for (hashes) |hash| {
        const matches = std.sort.equalRange(Item, Item{ .hash = hash, .id = 0 }, items, {}, Item.cmpByHash);
        for (matches[0]..matches[1]) |i| {
            try results.incr(items[i].id, self.id.version);
        }
        items = items[matches[1]..];
    }
}

pub fn ensureSorted(self: *Self) void {
    std.sort.pdq(Item, self.items.items, {}, Item.cmp);
}

pub fn canBeMerged(self: Self) bool {
    return !self.frozen;
}

pub fn getSize(self: Self) usize {
    return self.items.items.len;
}

pub fn merge(self: *Self, source1: *Self, source2: *Self, parent: List) !void {
    const sources = [2]*Self{ source1, source2 };

    self.id = common.SegmentID.merge(source1.id, source2.id);
    self.max_commit_id = @max(source1.max_commit_id, source2.max_commit_id);

    var total_docs: usize = 0;
    var total_items: usize = 0;
    for (sources) |segment| {
        total_docs += segment.docs.count();
        total_items += segment.items.items.len;
    }

    try self.docs.ensureUnusedCapacity(@truncate(total_docs));
    try self.items.ensureTotalCapacity(total_items);

    {
        var skip_docs = std.AutoHashMap(u32, void).init(self.allocator);
        defer skip_docs.deinit();

        try skip_docs.ensureTotalCapacity(@truncate(total_docs / 10));

        for (sources) |segment| {
            skip_docs.clearRetainingCapacity();

            var docs_iter = segment.docs.iterator();
            while (docs_iter.next()) |entry| {
                const id = entry.key_ptr.*;
                const status = entry.value_ptr.*;
                if (!parent.hasNewerVersion(id, segment.id.version)) {
                    try self.docs.put(id, status);
                } else {
                    try skip_docs.put(id, {});
                }
            }

            for (segment.items.items) |item| {
                if (!skip_docs.contains(item.id)) {
                    try self.items.append(item);
                }
            }
        }
    }

    self.ensureSorted();
}
