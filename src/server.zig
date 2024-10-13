const std = @import("std");
const httpz = @import("httpz");
const json = std.json;
const log = std.log.scoped(.server);

const zul = @import("zul");

const Index = @import("Index.zig");
const common = @import("common.zig");
const SearchResults = common.SearchResults;
const Change = common.Change;
const Deadline = @import("utils/Deadline.zig");

const Context = struct {
    index: *Index,
};

fn run(index: *Index, address: []const u8, port: u16, threads: u16) !void {
    var ctx = Context{ .index = index };

    const config = httpz.Config{
        .address = address,
        .port = port,
        .thread_pool = .{
            .count = threads,
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

    var index = Index.init(allocator, dir);
    defer index.deinit();

    try index.start();

    try run(&index, address, port, threads);
}

test {
    std.testing.refAllDecls(@This());
}
