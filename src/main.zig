const std = @import("std");
const zio = @import("zio");
const http = @import("dusty");

// NOTE: intentionally NOT setting `std_options_debug_io = zio.debug_io` for now
// due to zio#545 (panics recurse through debug_io and abort, masking the real
// panic). Re-enable once that's fixed.

const MultiIndex = @import("MultiIndex.zig");
const Server = @import("server.zig").Server;
const registerRoutes = @import("server.zig").registerRoutes;

const Config = struct {
    dir: []const u8 = "data",
    host: []const u8 = "127.0.0.1",
    port: u16 = 8080,
};

fn runServer(allocator: std.mem.Allocator, rt: *zio.Runtime, config: Config) !void {
    const io = rt.io();

    const cwd = zio.Dir.cwd();
    const data_dir = cwd.openDir(config.dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => blk: {
            try cwd.createDir(config.dir, 0o755);
            break :blk try cwd.openDir(config.dir, .{ .iterate = true });
        },
        else => return err,
    };
    var multi_index = MultiIndex.init(allocator, data_dir);
    defer multi_index.deinit();
    try multi_index.open();

    var server = Server.init(allocator, io, .{}, &multi_index);
    defer server.deinit();

    registerRoutes(&server);

    var sigint = try zio.Signal.init(.interrupt);
    defer sigint.deinit();
    var sigterm = try zio.Signal.init(.terminate);
    defer sigterm.deinit();

    const addr: http.Address = .{ .ip = try std.Io.net.IpAddress.parse(config.host, config.port) };
    std.log.info("fpindex-ng listening on http://{s}:{d} (dir={s})", .{ config.host, config.port, config.dir });

    var task = try zio.spawn(Server.listen, .{ &server, addr });
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

fn parseArgs(args: std.process.Args) !Config {
    var config = Config{};
    var it = std.process.Args.Iterator.init(args);
    _ = it.skip(); // argv[0]
    while (it.next()) |arg| {
        if (std.mem.eql(u8, arg, "--dir")) {
            config.dir = it.next() orelse return error.MissingArgument;
        } else if (std.mem.eql(u8, arg, "--host")) {
            config.host = it.next() orelse return error.MissingArgument;
        } else if (std.mem.eql(u8, arg, "--port")) {
            config.port = try std.fmt.parseInt(u16, it.next() orelse return error.MissingArgument, 10);
        } else {
            std.log.warn("ignoring unknown argument '{s}'", .{arg});
        }
    }
    return config;
}

pub fn main(init: std.process.Init.Minimal) !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const config = try parseArgs(init.args);

    var rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    var task = try zio.spawn(runServer, .{ allocator, rt, config });
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
