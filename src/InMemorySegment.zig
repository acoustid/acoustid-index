const std = @import("std");
const log = std.log;
const io = std.io;
const assert = std.debug.assert;

const common = @import("common.zig");
const Item = common.Item;
const SearchResults = common.SearchResults;

const Deadline = @import("utils/Deadline.zig");

const filefmt = @import("filefmt.zig");

const Self = @This();

allocator: std.mem.Allocator,
version: u32,
docs: std.AutoHashMap(u32, bool),
items: std.ArrayList(Item),
frozen: bool = false,
merged: u32 = 0,

pub fn init(allocator: std.mem.Allocator) Self {
    return .{
        .allocator = allocator,
        .docs = std.AutoHashMap(u32, bool).init(allocator),
        .items = std.ArrayList(Item).init(allocator),
        .version = 0,
    };
}

pub fn deinit(self: *Self) void {
    self.docs.deinit();
    self.items.deinit();
}

pub fn write(self: *Self, writer: anytype) !void {
    if (self.version == 0) {
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
            try results.incr(items[i].id, self.version);
        }
        items = items[matches[1]..];
    }
}

pub fn ensureSorted(self: *Self) void {
    std.sort.pdq(Item, self.items.items, {}, Item.cmp);
}
