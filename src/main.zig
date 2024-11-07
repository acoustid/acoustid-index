const std = @import("std");
const zul = @import("zul");

const MultiIndex = @import("MultiIndex.zig");
const Scheduler = @import("utils/Scheduler.zig");
const server = @import("server.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var args = try zul.CommandLineArgs.parse(allocator);
    defer args.deinit();

    const dir_path = args.get("dir") orelse ".";
    const dir = try std.fs.cwd().openDir(dir_path, .{});

    const address = args.get("address") orelse "127.0.0.1";

    const port_str = args.get("port") orelse "8080";
    const port = try std.fmt.parseInt(u16, port_str, 10);

    _ = try std.net.Address.parseIp(address, port);

    const threads_str = args.get("threads") orelse "0";
    var threads = try std.fmt.parseInt(u16, threads_str, 10);
    if (threads == 0) {
        threads = @intCast(try std.Thread.getCpuCount());
    }

    var scheduler = Scheduler.init(allocator);
    defer scheduler.deinit();

    var indexes = MultiIndex.init(allocator, dir, &scheduler);
    defer indexes.deinit();

    try scheduler.start(4);
    defer scheduler.stop();

    try server.run(allocator, &indexes, address, port, threads);
}

test {
    std.testing.refAllDecls(@This());
}
