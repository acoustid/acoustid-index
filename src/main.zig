const std = @import("std");
const zio = @import("zio");
const http = @import("dusty");

fn handleHealth(_: *http.Request, res: *http.Response) !void {
    res.body = "OK\n";
}

fn runServer(allocator: std.mem.Allocator, rt: *zio.Runtime) !void {
    const io = rt.io();

    var server = http.Server(void).init(allocator, io, .{}, {});
    defer server.deinit();

    server.router.get("/_health", handleHealth);

    var sigint = try zio.Signal.init(.interrupt);
    defer sigint.deinit();
    var sigterm = try zio.Signal.init(.terminate);
    defer sigterm.deinit();

    const addr: http.Address = .{ .ip = try std.Io.net.IpAddress.parse("127.0.0.1", 8080) };
    std.log.info("fpindex-ng listening on http://127.0.0.1:8080", .{});

    var task = try zio.spawn(http.Server(void).listen, .{ &server, addr });
    defer task.cancel();

    const result = try zio.select(.{ .task = &task, .sigint = &sigint, .sigterm = &sigterm });
    switch (result) {
        .task => |r| return r,
        .sigint, .sigterm => {
            std.log.info("shutting down", .{});
            task.cancel();
        },
    }
}

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    var task = try zio.spawn(runServer, .{ allocator, rt });
    try task.join();
}

test {
    _ = @import("segment.zig");
    _ = @import("streamvbyte.zig");
    _ = @import("block.zig");
    _ = @import("common.zig");
    _ = @import("Metadata.zig");
    _ = @import("segment_merge_policy.zig");
}
