const std = @import("std");

const InMemoryIndex = @import("InMemoryIndex.zig");

const common = @import("common.zig");
const SearchResults = common.SearchResults;
const Change = common.Change;

const Deadline = @import("utils/Deadline.zig");

const Segment = @import("Segment.zig");
const Segments = std.DoublyLinkedList(Segment);

const Self = @This();

allocator: std.mem.Allocator,
stage: InMemoryIndex,
segments: Segments,

pub fn init(allocator: std.mem.Allocator) Self {
    return .{
        .allocator = allocator,
        .stage = InMemoryIndex.init(allocator),
        .segments = .{},
    };
}

pub fn deinit(self: *Self) void {
    self.stage.deinit();
    while (self.segments.popFirst()) |node| {
        node.data.deinit();
        self.allocator.destroy(node);
    }
}

pub fn update(self: *Self, changes: []const Change) !void {
    try self.stage.update(changes);
}

pub fn search(self: *Self, hashes: []const u32, results: *SearchResults, deadline: Deadline) !void {
    const sorted_hashes = try self.allocator.dupe(u32, hashes);
    defer self.allocator.free(sorted_hashes);
    std.sort.pdq(u32, sorted_hashes, {}, std.sort.asc(u32));

    var it = self.segments.first;
    while (it) |node| : (it = node.next) {
        if (deadline.isExpired()) {
            return error.Timeout;
        }
        try node.data.search(sorted_hashes, results);
    }

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
    try std.testing.expectEqual(1, result.?.id);
    try std.testing.expectEqual(3, result.?.score);
    try std.testing.expect(result.?.version != 0);
}
