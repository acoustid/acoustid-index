const builtin = @import("builtin");
const std = @import("std");
const log = std.log.scoped(.main);
const zul = @import("zul");
const nats = @import("nats");

const Scheduler = @import("utils/Scheduler.zig");
const MultiIndex = @import("MultiIndex.zig");
const ClusterMultiIndex = @import("ClusterMultiIndex.zig");
const server = @import("server.zig");
const metrics = @import("metrics.zig");

pub const std_options = std.Options{
    .log_level = .debug,
    .logFn = logHandler,
};

var current_log_level: std.log.Level = .info;

pub fn logHandler(
    comptime level: std.log.Level,
    comptime scope: @TypeOf(.EnumLiteral),
    comptime format: []const u8,
    args: anytype,
) void {
    if (@intFromEnum(level) <= @intFromEnum(current_log_level)) {
        const level_txt = comptime level.asText();
        const prefix2 = if (scope == .default) ": " else "(" ++ @tagName(scope) ++ "): ";
        const stderr = std.io.getStdErr().writer();
        var bw = std.io.bufferedWriter(stderr);
        const writer = bw.writer();

        std.debug.lockStdErr();
        defer std.debug.unlockStdErr();
        nosuspend {
            writer.print("{d:.3} " ++ level_txt ++ prefix2 ++ format ++ "\n", .{@as(f64, @floatFromInt(std.time.milliTimestamp())) / 1000.0} ++ args) catch return;
            bw.flush() catch return;
        }
    }
}

fn printHelp() !void {
    const stdout = std.io.getStdOut().writer();
    try stdout.print("Usage: aindex [options]\n", .{});
    try stdout.print("Options:\n", .{});
    try stdout.print("  --help                          Show this help message and exit\n", .{});
    try stdout.print("  --dir                           Directory to index\n", .{});
    try stdout.print("  --address                       Address to listen on\n", .{});
    try stdout.print("  --port                          Port to listen on\n", .{});
    try stdout.print("  --threads                       Number of threads to use\n", .{});
    try stdout.print("  --log-level                     Log level (debug, info, warn, error)\n", .{});
    try stdout.print("  --parallel-loading-threshold    Minimum segments to trigger parallel loading (default: 8)\n", .{});
    try stdout.print("  --cluster                       Enable cluster mode\n", .{});
    try stdout.print("  --nats-url URL                  NATS server URL\n", .{});
}

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .{};
    defer _ = gpa.deinit();

    const allocator = if (builtin.mode == .ReleaseFast and !builtin.is_test) std.heap.c_allocator else gpa.allocator();

    var args = try zul.CommandLineArgs.parse(allocator);
    defer args.deinit();

    if (args.contains("help") or args.contains("h")) {
        try printHelp();
        return;
    }

    if (args.get("log-level")) |log_level_name| {
        if (std.meta.stringToEnum(std.log.Level, log_level_name)) |log_level| {
            current_log_level = log_level;
        } else {
            return error.InvalidLogLevel;
        }
    }

    const dir_path_relative = args.get("dir") orelse "/tmp/fpindex";
    const dir = try std.fs.cwd().makeOpenPath(dir_path_relative, .{ .iterate = true });

    const dir_path = try dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir_path);

    log.info("using directory {s}", .{dir_path});

    const address = args.get("address") orelse "127.0.0.1";

    const port_str = args.get("port") orelse "6081";
    const port = try std.fmt.parseInt(u16, port_str, 10);

    _ = try std.net.Address.parseIp(address, port);

    const threads_str = args.get("threads") orelse "0";
    var threads = try std.fmt.parseInt(u16, threads_str, 10);
    if (threads == 0) {
        threads = @intCast(try std.Thread.getCpuCount());
    }
    log.info("using {} threads", .{threads});

    const parallel_loading_threshold_str = args.get("parallel-loading-threshold") orelse "8";
    const parallel_loading_threshold = try std.fmt.parseInt(usize, parallel_loading_threshold_str, 10);
    log.info("using parallel loading threshold of {}", .{parallel_loading_threshold});

    try metrics.initializeMetrics(allocator, .{ .prefix = "aindex_" });
    defer metrics.deinitMetrics();

    var scheduler = Scheduler.init(allocator);
    defer scheduler.deinit();

    try scheduler.start(threads);

    var indexes = MultiIndex.init(allocator, &scheduler, dir, .{
        .parallel_segment_loading_threshold = parallel_loading_threshold,
    });
    defer indexes.deinit();

    try indexes.open();

    const cluster_mode = args.contains("cluster");
    if (!cluster_mode) {
        try server.run(MultiIndex, allocator, &indexes, address, port, threads);
        return;
    }

    const url = args.get("nats-url") orelse "nats://localhost:4222";
    log.info("connecting to NATS at {s}", .{url});

    var nc = nats.Connection.init(allocator, .{});
    defer nc.deinit();

    try nc.connect(url);

    var cluster = try ClusterMultiIndex.init(allocator, &nc, &indexes);
    defer cluster.deinit();
    
    // Start consumer thread
    try cluster.start();

    try server.run(ClusterMultiIndex, allocator, &cluster, address, port, threads);
}

test {
    std.testing.refAllDecls(@This());
}
