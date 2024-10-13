const std = @import("std");
const log = std.log.scoped(.main);

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

    const port_str = args.get("port") orelse "8080";
    const port = try std.fmt.parseInt(u16, port_str, 10);

    _ = try std.net.Address.parseIp(address, port);

    const threads_str = args.get("threads") orelse "0";
    var threads = try std.fmt.parseInt(u16, threads_str, 10);
    if (threads == 0) {
        threads = @intCast(try std.Thread.getCpuCount());
    }

    try server.run(&index, address, port, threads);
}
