const std = @import("std");

const common = @import("common.zig");
const SearchResults = common.SearchResults;

const Index = @import("Index.zig");

const server = @import("server.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    var index = Index.init(allocator);
    defer index.deinit();

    try server.run(&index, "127.0.0.1", 8080);
}
