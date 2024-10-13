const std = @import("std");
const zul = @import("zul");

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

    var args = try zul.CommandLineArgs.parse(allocator);
    defer args.deinit();

    const address = args.get("address") orelse "127.0.0.1";
    const port = try std.fmt.parseInt(u16, args.get("port") orelse "8080", 10);

    try server.run(&index, address, port);
}
