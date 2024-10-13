const std = @import("std");

const InMemoryIndex = @import("InMemoryIndex.zig");

const common = @import("common.zig");
const SearchResults = common.SearchResults;
const Change = common.Change;

const Deadline = @import("utils/Deadline.zig");

const Self = @This();

allocator: std.mem.Allocator,
stage: InMemoryIndex,

pub fn init(allocator: std.mem.Allocator) Self {
    return .{
        .allocator = allocator,
        .stage = InMemoryIndex.init(allocator),
    };
}

pub fn deinit(self: *Self) void {
    self.stage.deinit();
}

pub fn update(self: *Self, changes: []const Change) !void {
    try self.stage.update(changes);
}

pub fn search(self: *Self, hashes: []const u32, results: *SearchResults, deadline: Deadline) !void {
    const sorted_hashes = try self.allocator.dupe(u32, hashes);
    defer self.allocator.free(sorted_hashes);
    std.sort.pdq(u32, sorted_hashes, {}, std.sort.asc(u32));
    try self.stage.search(sorted_hashes, results, deadline);
    results.sort();
}

test "insert and search" {
    var index = Self.init(std.testing.allocator);
    defer index.deinit();

    try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }});

    var results = SearchResults.init(std.testing.allocator);
    defer results.deinit();

    try index.search(&[_]u32{ 1, 2, 3 }, &results, .{});

    try std.testing.expectEqual(1, results.count());

    const result = results.get(1);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(1, result.?.docId);
    try std.testing.expectEqual(3, result.?.score);
    try std.testing.expect(result.?.version != 0);
}
