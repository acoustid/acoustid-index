const std = @import("std");
const httpz = @import("httpz");
const json = std.json;
const log = std.log.scoped(.server);

const Index = @import("Index.zig");
const common = @import("common.zig");
const SearchResults = common.SearchResults;
const Change = common.Change;
const Deadline = @import("utils/Deadline.zig");

const Context = struct {
    index: *Index,
};

pub fn run(index: *Index, address: []const u8, port: u16) !void {
    var ctx = Context{ .index = index };

    const config = httpz.Config{
        .address = address,
        .port = port,
        .thread_pool = .{
            .count = @intCast(try std.Thread.getCpuCount()),
        },
    };

    var server = try httpz.ServerApp(*Context).init(index.allocator, config, &ctx);
    defer {
        server.stop();
        server.deinit();
    }

    var router = server.router();
    router.post("/_search", handleSearch);
    router.post("/_update", handleUpdate);

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

    ctx.index.search(body.query, &results, deadline) catch |err| {
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
    const body_or_null = req.json(UpdateRequestJSON) catch {
        res.status = 400;
        return res.json(.{ .status = "invalid body" }, .{});
    };
    if (body_or_null == null) {
        res.status = 400;
        return res.json(.{ .status = "invalid body" }, .{});
    }

    const body = body_or_null.?;

    ctx.index.update(body.changes) catch |err| {
        log.err("index search error: {}", .{err});
        res.status = 500;
        return res.json(.{ .status = "internal error" }, .{});
    };

    return res.json(.{ .status = "ok" }, .{});
}
