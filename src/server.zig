const std = @import("std");
const httpz = @import("httpz");
const json = std.json;
const log = std.log.scoped(.server);

const MultiIndex = @import("MultiIndex.zig");
const IndexData = MultiIndex.IndexRef;
const common = @import("common.zig");
const SearchResults = common.SearchResults;
const Change = common.Change;
const Deadline = @import("utils/Deadline.zig");
const Scheduler = @import("utils/Scheduler.zig");

const m = @import("metrics.zig");

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

fn installSignalHandlers(server: *Server) !void {
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

pub fn run(allocator: std.mem.Allocator, indexes: *MultiIndex, address: []const u8, port: u16, threads: u16) !void {
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

    try installSignalHandlers(&server);

    var router = server.router();

    // Monitoring API
    router.get("/_ping", handlePing);
    router.get("/_metrics", handleMetrics);

    // Search API
    router.post("/:index/_search", handleSearch);

    // Bulk API
    router.post("/:index/_update", handleUpdate);

    // Index API
    router.head("/:index", handleHeadIndex);
    router.get("/:index", handleGetIndex);
    router.put("/:index", handlePutIndex);
    router.delete("/:index", handleDeleteIndex);

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

fn getIndexNo(ctx: *Context, req: *httpz.Request, res: *httpz.Response, send_body: bool) !?u8 {
    _ = ctx;
    const index_no_str = req.param("index") orelse "0";
    const index_no = std.fmt.parseInt(u8, index_no_str, 10) catch {
        res.status = 400;
        if (send_body) {
            try res.json(.{ .status = "invalid index number" }, .{});
        }
        return null;
    };
    return index_no;
}

fn getIndex(ctx: *Context, req: *httpz.Request, res: *httpz.Response, send_body: bool) !?*IndexData {
    const index_no = try getIndexNo(ctx, req, res, send_body) orelse return null;
    const index = ctx.indexes.getIndex(index_no) catch |err| {
        if (err == error.IndexNotFound) {
            res.status = 404;
            if (send_body) {
                try res.json(.{ .status = "index not found" }, .{});
            }
            return null;
        }
        return err;
    };
    return index;
}

fn releaseIndex(ctx: *Context, index: *IndexData) void {
    ctx.indexes.releaseIndex(index);
}

fn getJsonBody(comptime T: type, req: *httpz.Request, res: *httpz.Response) !?T {
    const body_or_null = req.json(T) catch {
        res.status = 400;
        try res.json(.{ .status = "invalid body" }, .{});
        return null;
    };
    if (body_or_null == null) {
        res.status = 400;
        try res.json(.{ .status = "invalid body" }, .{});
        return null;
    }
    return body_or_null.?;
}

fn handleSearch(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const body = try getJsonBody(SearchRequestJSON, req, res) orelse return;

    const index_ref = try getIndex(ctx, req, res, true) orelse return;
    const index = &index_ref.index;
    defer releaseIndex(ctx, index_ref);

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

    m.metrics.searches.incr();

    index.search(body.query, &results, deadline) catch |err| {
        log.err("index search error: {}", .{err});
        res.status = 500;
        return res.json(.{ .status = "internal error" }, .{});
    };

    var results_json = SearchResultsJSON{ .results = try req.arena.alloc(SearchResultJSON, results.count()) };
    for (results.values(), 0..) |r, i| {
        if (r.score > 0) {
            results_json.results[i] = SearchResultJSON{ .id = r.id, .score = r.score };
        }
    }
    return res.json(results_json, .{});
}

const UpdateRequestJSON = struct {
    changes: []Change,
};

fn handleUpdate(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const body = try getJsonBody(UpdateRequestJSON, req, res) orelse return;

    const index_ref = try getIndex(ctx, req, res, true) orelse return;
    const index = &index_ref.index;
    defer releaseIndex(ctx, index_ref);

    index.update(body.changes) catch |err| {
        log.err("index search error: {}", .{err});
        res.status = 500;
        return res.json(.{ .status = "internal error" }, .{});
    };

    return res.json(.{ .status = "ok" }, .{});
}

fn handleHeadIndex(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_ref = try getIndex(ctx, req, res, false) orelse return;
    defer releaseIndex(ctx, index_ref);

    res.status = 200;
    return;
}

const GetIndexResponse = struct {
    status: []const u8,
    max_commit_id: u64,
};

fn handleGetIndex(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_ref = try getIndex(ctx, req, res, true) orelse return;
    const index = &index_ref.index;
    defer releaseIndex(ctx, index_ref);

    const response = GetIndexResponse{
        .status = "ok",
        .max_commit_id = index.getMaxCommitId(),
    };
    return res.json(&response, .{});
}

fn handlePutIndex(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_no = try getIndexNo(ctx, req, res, true) orelse return;

    ctx.indexes.createIndex(index_no) catch |err| {
        log.err("index create error: {}", .{err});
        res.status = 500;
        return res.json(.{ .status = "internal error" }, .{});
    };

    return res.json(.{ .status = "ok" }, .{});
}

fn handleDeleteIndex(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_no = try getIndexNo(ctx, req, res, true) orelse return;

    ctx.indexes.deleteIndex(index_no) catch |err| {
        log.err("index delete error: {}", .{err});
        res.status = 500;
        return res.json(.{ .status = "internal error" }, .{});
    };

    return res.json(.{ .status = "ok" }, .{});
}

fn handlePing(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    _ = ctx;
    _ = req;

    try res.writer().writeAll("pong\n");
}

fn handleMetrics(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    _ = ctx;
    _ = req;

    try m.writeMetrics(res.writer());
}
