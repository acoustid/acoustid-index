const std = @import("std");
const httpz = @import("httpz");
const json = std.json;
const log = std.log.scoped(.server);

const zul = @import("zul");

const MultiIndex = @import("MultiIndex.zig");
const common = @import("common.zig");
const SearchResults = common.SearchResults;
const Change = common.Change;
const Deadline = @import("utils/Deadline.zig");

const Context = struct {
    indexes: *MultiIndex,
};

const Server = httpz.ServerApp(*Context);

var global_server: ?*Server = null;

fn shutdown(_: c_int) callconv(.C) void {
    if (global_server) |server| {
        log.info("stopping", .{});
        global_server = null;
        server.stop();
    }
}

fn install_signal_handlers(server: *Server) !void {
    global_server = server;

    try std.posix.sigaction(std.posix.SIG.INT, &.{
        .handler = .{ .handler = shutdown },
        .mask = std.posix.empty_sigset,
        .flags = 0,
    }, null);

    try std.posix.sigaction(std.posix.SIG.TERM, &.{
        .handler = .{ .handler = shutdown },
        .mask = std.posix.empty_sigset,
        .flags = 0,
    }, null);
}

fn run(allocator: std.mem.Allocator, indexes: *MultiIndex, address: []const u8, port: u16, threads: u16) !void {
    var ctx = Context{ .indexes = indexes };

    const config = httpz.Config{
        .address = address,
        .port = port,
        .thread_pool = .{
            .count = threads,
        },
    };

    var server = try Server.init(allocator, config, &ctx);
    defer server.deinit();

    try install_signal_handlers(&server);

    var router = server.router();
    router.post("/_search", handleSearch);
    router.post("/_update", handleUpdate);
    router.post("/:index/_search", handleSearch);
    router.post("/:index/_update", handleUpdate);

    log.info("listening on {s}:{d}", .{ address, port });
    try server.listen();
}

const default_search_timeout = 500;
const max_search_timeout = 10000;

const SearchRequestJSON = struct {
    query: []const u32,
    timeout: u32 = 0,
};

const SearchResultJSON = struct {
    id: u32,
    score: u32,
};

const SearchResultsJSON = struct {
    results: []SearchResultJSON,
};

fn handleSearch(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_no_str = req.param("index") orelse "0";
    const index_no = std.fmt.parseInt(u8, index_no_str, 10) catch {
        res.status = 400;
        return res.json(.{ .status = "invalid index number" }, .{});
    };
    const index_ref = try ctx.indexes.getIndex(index_no);
    const index = &index_ref.index;
    defer ctx.indexes.releaseIndex(index_ref);

    const body_or_null = req.json(SearchRequestJSON) catch {
        res.status = 400;
        return res.json(.{ .status = "invalid body" }, .{});
    };
    if (body_or_null == null) {
        res.status = 400;
        return res.json(.{ .status = "invalid body" }, .{});
    }

    const body = body_or_null.?;

    var results = SearchResults.init(req.arena);
    defer results.deinit();

    var timeout = body.timeout;
    if (timeout == 0) {
        timeout = default_search_timeout;
    }
    if (timeout > max_search_timeout) {
        timeout = max_search_timeout;
    }
    const deadline = Deadline.init(timeout);

    index.search(body.query, &results, deadline) catch |err| {
        log.err("index search error: {}", .{err});
        res.status = 500;
        return res.json(.{ .status = "internal error" }, .{});
    };

    var results_json = SearchResultsJSON{ .results = try req.arena.alloc(SearchResultJSON, results.count()) };
    for (results.values(), 0..) |r, i| {
        results_json.results[i] = SearchResultJSON{ .id = r.id, .score = r.score };
    }
    return res.json(results_json, .{});
}

const UpdateRequestJSON = struct {
    changes: []Change,
};

fn handleUpdate(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_no_str = req.param("index") orelse "0";
    const index_no = std.fmt.parseInt(u8, index_no_str, 10) catch {
        res.status = 400;
        return res.json(.{ .status = "invalid index number" }, .{});
    };
    const index_ref = try ctx.indexes.getIndex(index_no);
    const index = &index_ref.index;
    defer ctx.indexes.releaseIndex(index_ref);

    const body_or_null = req.json(UpdateRequestJSON) catch {
        res.status = 400;
        return res.json(.{ .status = "invalid body" }, .{});
    };
    if (body_or_null == null) {
        res.status = 400;
        return res.json(.{ .status = "invalid body" }, .{});
    }

    const body = body_or_null.?;

    index.update(body.changes) catch |err| {
        log.err("index search error: {}", .{err});
        res.status = 500;
        return res.json(.{ .status = "internal error" }, .{});
    };

    return res.json(.{ .status = "ok" }, .{});
}

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

    var indexes = MultiIndex.init(allocator, dir);
    defer indexes.deinit();

    try run(allocator, &indexes, address, port, threads);
}

test {
    std.testing.refAllDecls(@This());
}
