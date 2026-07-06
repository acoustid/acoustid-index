const std = @import("std");
const builtin = @import("builtin");
const zio = @import("zio");
const http = @import("dusty");

// NOTE: intentionally NOT setting `std_options_debug_io = zio.debug_io` for now
// due to zio#545 (panics recurse through debug_io and abort, masking the real
// panic). Re-enable once that's fixed.

const MultiIndex = @import("MultiIndex.zig");
const metrics = @import("metrics.zig");
const legacy = @import("legacy.zig");
const coordinator_server = @import("coordinator_server.zig");
const MemoryCoordinator = @import("Coordinator.zig").MemoryCoordinator;
const RemoteCoordinator = @import("RemoteCoordinator.zig");
const Server = @import("server.zig").Server;
const registerRoutes = @import("server.zig").registerRoutes;

// Process allocator: a leak-checking DebugAllocator in Debug builds, the C
// allocator (malloc) otherwise — malloc scales better than a mutex-guarded GPA
// under many threads.
const RootAllocator = struct {
    const use_debug = builtin.mode == .Debug;
    gpa: if (use_debug) std.heap.DebugAllocator(.{}) else void,

    fn init() RootAllocator {
        return .{ .gpa = if (use_debug) .{} else {} };
    }

    fn allocator(self: *RootAllocator) std.mem.Allocator {
        return if (use_debug) self.gpa.allocator() else std.heap.c_allocator;
    }

    fn deinit(self: *RootAllocator) void {
        if (use_debug) _ = self.gpa.deinit();
    }
};

const Config = struct {
    dir: []const u8 = "data",
    host: []const u8 = "127.0.0.1",
    port: u16 = 8080,
    checkpoint_threshold: usize = 100_000,
    // Force a checkpoint once the oldest uncheckpointed write is this old (ms); 0
    // disables age-based checkpointing. Keeps file_version fresh for snapshot bootstrap.
    checkpoint_age_ms: u64 = 60_000,
    // Legacy line-protocol listener; 0 disables it.
    legacy_port: u16 = 0,
    // Max file-segment loads in flight across all indexes at startup; 0 = no limit.
    load_concurrency: usize = 0,
    // Run as the changelog coordinator (serves append/read) instead of an index.
    coordinator: bool = false,
    // Replica mode: consume the changelog from this coordinator URL.
    coordinator_url: ?[]const u8 = null,
};

// Bulk `_update` batches (and the coordinator's append bodies, which carry the same
// changes) run well past dusty's 1MB default, so raise the body cap to 16MB (matching
// the previous version).
const server_config: http.ServerConfig = .{ .request = .{ .max_body_size = 16 * 1024 * 1024 } };

fn runServer(allocator: std.mem.Allocator, rt: *zio.Runtime, config: Config) !void {
    const io = rt.io();

    if (config.coordinator) return runCoordinator(allocator, io, config);

    const cwd = zio.Dir.cwd();
    const data_dir = cwd.openDir(config.dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => blk: {
            try cwd.createDir(config.dir, 0o755);
            break :blk try cwd.openDir(config.dir, .{ .iterate = true });
        },
        else => return err,
    };

    // In replica mode the changelog must outlive the index manager (its consumers
    // borrow it), so create it first — LIFO defers then tear mi down before it.
    var remote: ?RemoteCoordinator = if (config.coordinator_url) |url|
        RemoteCoordinator.init(allocator, io, url)
    else
        null;
    defer if (remote) |*r| r.deinit();

    var multi_index = MultiIndex.init(allocator, data_dir);
    multi_index.checkpoint_threshold = config.checkpoint_threshold;
    multi_index.checkpoint_age = if (config.checkpoint_age_ms == 0) null else .fromMilliseconds(config.checkpoint_age_ms);
    multi_index.load_concurrency = config.load_concurrency;
    defer multi_index.deinit();
    try multi_index.open();

    if (remote) |*r| {
        try multi_index.startReplication(r.coordinator());
        std.log.info("replicating from coordinator {s}", .{config.coordinator_url.?});
    }

    var server = Server.init(allocator, io, server_config, &multi_index);
    defer server.deinit();

    registerRoutes(&server);

    var sigint = try zio.Signal.init(.interrupt);
    defer sigint.deinit();
    var sigterm = try zio.Signal.init(.terminate);
    defer sigterm.deinit();

    const addr: http.Address = .{ .ip = try std.Io.net.IpAddress.parse(config.host, config.port) };
    std.log.info("fpindex-ng listening on http://{s}:{d} (dir={s})", .{ config.host, config.port, config.dir });

    var http_task = try zio.spawn(Server.listen, .{ &server, addr });
    defer http_task.cancel();

    if (config.legacy_port != 0) {
        const legacy_addr = try zio.net.IpAddress.parseIp4(config.host, config.legacy_port);
        var legacy_task = try zio.spawn(legacy.listen, .{ &multi_index, legacy_addr, config.coordinator_url != null });
        defer legacy_task.cancel();

        const result = try zio.select(.{ .http = &http_task, .legacy = &legacy_task, .sigint = &sigint, .sigterm = &sigterm });
        switch (result) {
            .http => |r| return r,
            .legacy => |r| return r,
            .sigint, .sigterm => {
                std.log.info("shutting down", .{});
                http_task.cancel();
                legacy_task.cancel();
            },
        }
    } else {
        const result = try zio.select(.{ .http = &http_task, .sigint = &sigint, .sigterm = &sigterm });
        switch (result) {
            .http => |r| return r,
            .sigint, .sigterm => {
                std.log.info("shutting down", .{});
                http_task.cancel();
            },
        }
    }
}

// Run as the changelog coordinator: an HTTP server exposing append/read of an
// in-memory changelog for replicas to consume. No index, no data dir.
fn runCoordinator(allocator: std.mem.Allocator, io: std.Io, config: Config) !void {
    var mc = MemoryCoordinator.init(allocator);
    defer mc.deinit();
    var co = coordinator_server.Service{ .coordinator = mc.coordinator() };

    var server = coordinator_server.Server.init(allocator, io, server_config, &co);
    defer server.deinit();
    coordinator_server.registerRoutes(&server);

    var sigint = try zio.Signal.init(.interrupt);
    defer sigint.deinit();
    var sigterm = try zio.Signal.init(.terminate);
    defer sigterm.deinit();

    const addr: http.Address = .{ .ip = try std.Io.net.IpAddress.parse(config.host, config.port) };
    std.log.info("fpindex coordinator listening on http://{s}:{d}", .{ config.host, config.port });

    var http_task = try zio.spawn(coordinator_server.Server.listen, .{ &server, addr });
    defer http_task.cancel();

    const result = try zio.select(.{ .http = &http_task, .sigint = &sigint, .sigterm = &sigterm });
    switch (result) {
        .http => |r| return r,
        .sigint, .sigterm => {
            std.log.info("shutting down", .{});
            http_task.cancel();
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
        } else if (std.mem.eql(u8, arg, "--checkpoint-threshold")) {
            config.checkpoint_threshold = try std.fmt.parseInt(usize, it.next() orelse return error.MissingArgument, 10);
        } else if (std.mem.eql(u8, arg, "--checkpoint-age-ms")) {
            config.checkpoint_age_ms = try std.fmt.parseInt(u64, it.next() orelse return error.MissingArgument, 10);
        } else if (std.mem.eql(u8, arg, "--legacy-port")) {
            config.legacy_port = try std.fmt.parseInt(u16, it.next() orelse return error.MissingArgument, 10);
        } else if (std.mem.eql(u8, arg, "--load-concurrency")) {
            config.load_concurrency = try std.fmt.parseInt(usize, it.next() orelse return error.MissingArgument, 10);
        } else if (std.mem.eql(u8, arg, "--coordinator")) {
            config.coordinator = true;
        } else if (std.mem.eql(u8, arg, "--coordinator-url")) {
            config.coordinator_url = it.next() orelse return error.MissingArgument;
        } else {
            std.log.warn("ignoring unknown argument '{s}'", .{arg});
        }
    }
    return config;
}

pub fn main(init: std.process.Init.Minimal) !void {
    var root_allocator = RootAllocator.init();
    defer root_allocator.deinit();
    const allocator = root_allocator.allocator();

    const config = try parseArgs(init.args);

    metrics.init(.{});

    // Multiple executors so searches, updates, and merges spread across cores.
    // The index is thread-safe: reads work on a refcounted snapshot, the segment
    // pointer is swapped under a lock, and refcounts are atomic.
    var rt = try zio.Runtime.init(allocator, .{ .executors = .auto });
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
    _ = @import("filefmt.zig");
    _ = @import("Index.zig");
    _ = @import("Coordinator.zig");
    _ = @import("Replicator.zig");
    _ = @import("coordinator_server.zig");
    _ = @import("RemoteCoordinator.zig");
    _ = @import("index_redirect.zig");
}
