const std = @import("std");

const common = @import("common.zig");
const SearchResults = common.SearchResults;

const Index = @import("Index.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    var index = Index.init(allocator);
    defer index.deinit();

    try index.update(&[_]common.Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }});

    var results = SearchResults.init(allocator);
    defer results.deinit();

    try index.search(&[_]u32{ 1, 2, 3 }, &results, .{});

    for (results.values()) |result| {
        std.debug.print("docId: {}, score: {}, version: {}\n", .{ result.docId, result.score, result.version });
    }
}

test {
    _ = @import("filefmt.zig");
    std.testing.refAllDeclsRecursive(@This());
}
