const std = @import("std");
const httpz = @import("httpz");
const json = std.json;
const log = std.log.scoped(.server);

const msgpack = @import("msgpack");

const MultiIndex = @import("MultiIndex.zig");
const IndexData = MultiIndex.IndexRef;
const common = @import("common.zig");
const SearchResults = common.SearchResults;
const Change = @import("change.zig").Change;
const Deadline = @import("utils/Deadline.zig");

const metrics = @import("metrics.zig");

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
        .timeout = .{
            .request = 60,
            .keepalive = 300,
        },
    };

    var server = try Server.init(allocator, config, &ctx);
    defer server.deinit();

    server.errorHandler(handleError);
    server.notFound(handleNotFound);

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

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

const SearchResultJSON = struct {
    id: u32,
    score: u32,
};

const SearchResultsJSON = struct {
    results: []SearchResultJSON,
};

fn getIndex(ctx: *Context, req: *httpz.Request, res: *httpz.Response, send_body: bool) !?*IndexData {
    const index_name = req.param("index") orelse return null;
    const index = ctx.indexes.getIndex(index_name) catch |err| {
        if (err == error.IndexNotFound) {
            if (send_body) {
                try writeErrorResponse(400, err, req, res);
            } else {
                res.status = 404;
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

const ContentType = enum {
    json,
    msgpack,
};

fn parseContentTypeHeader(req: *httpz.Request) !ContentType {
    if (req.header("content-type")) |content_type| {
        if (std.mem.eql(u8, content_type, "application/json")) {
            return .json;
        } else if (std.mem.eql(u8, content_type, "application/vnd.msgpack")) {
            return .msgpack;
        }
        return error.InvalidContentType;
    }
    return .json;
}

fn parseAcceptHeader(req: *httpz.Request) ContentType {
    if (req.header("accept")) |accept_header| {
        if (std.mem.eql(u8, accept_header, "application/json")) {
            return .json;
        } else if (std.mem.eql(u8, accept_header, "application/vnd.msgpack")) {
            return .msgpack;
        }
    }
    return .json;
}

fn writeResponse(value: anytype, req: *httpz.Request, res: *httpz.Response) !void {
    const content_type = parseAcceptHeader(req);

    switch (content_type) {
        .json => try res.json(value, .{}),
        .msgpack => {
            res.header("content-type", "application/vnd.msgpack");
            try msgpack.encode(value, res.writer());
        },
    }
}

const ErrorResponse = struct {
    @"error": []const u8,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

fn handleNotFound(_: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    try writeErrorResponse(404, error.NotFound, req, res);
}

fn handleError(_: *Context, req: *httpz.Request, res: *httpz.Response, err: anyerror) void {
    log.err("unhandled error in {s}: {any}", .{ req.url.raw, err });
    writeErrorResponse(500, err, req, res) catch {
        res.status = 500;
        res.body = "internal error";
    };
}

fn writeErrorResponse(status: u16, err: anyerror, req: *httpz.Request, res: *httpz.Response) !void {
    res.status = status;
    try writeResponse(ErrorResponse{ .@"error" = @errorName(err) }, req, res);
}

fn getRequestBody(comptime T: type, req: *httpz.Request, res: *httpz.Response) !?T {
    const content = req.body() orelse {
        try writeErrorResponse(400, error.NoContent, req, res);
        return null;
    };

    const content_type = parseContentTypeHeader(req) catch {
        try writeErrorResponse(415, error.UnsupportedContentType, req, res);
        return null;
    };

    switch (content_type) {
        .json => {
            return json.parseFromSliceLeaky(T, req.arena, content, .{}) catch {
                try writeErrorResponse(400, error.InvalidContent, req, res);
                return null;
            };
        },
        .msgpack => {
            return msgpack.decodeFromSliceLeaky(T, req.arena, content) catch {
                try writeErrorResponse(400, error.InvalidContent, req, res);
                return null;
            };
        },
    }

    unreachable;
}

fn handleSearch(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const body = try getRequestBody(SearchRequestJSON, req, res) orelse return;

    const index_ref = try getIndex(ctx, req, res, true) orelse return;
    const index = &index_ref.index;
    defer releaseIndex(ctx, index_ref);

    var timeout = body.timeout;
    if (timeout == 0) {
        timeout = default_search_timeout;
    }
    if (timeout > max_search_timeout) {
        timeout = max_search_timeout;
    }
    const deadline = Deadline.init(timeout);

    metrics.search();

    const results = try index.search(body.query, req.arena, deadline);

    var results_json = SearchResultsJSON{ .results = try req.arena.alloc(SearchResultJSON, results.count()) };
    for (results.values(), 0..) |r, i| {
        if (r.score > 0) {
            results_json.results[i] = SearchResultJSON{ .id = r.id, .score = r.score };
        }
    }
    return writeResponse(results_json, req, res);
}

const UpdateRequestJSON = struct {
    changes: []Change,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

fn handleUpdate(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const body = try getRequestBody(UpdateRequestJSON, req, res) orelse return;

    const index_ref = try getIndex(ctx, req, res, true) orelse return;
    const index = &index_ref.index;
    defer releaseIndex(ctx, index_ref);

    metrics.update(body.changes.len);

    try index.update(body.changes);

    return writeResponse(.{ .status = "ok" }, req, res);
}

fn handleHeadIndex(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_ref = try getIndex(ctx, req, res, false) orelse return;
    defer releaseIndex(ctx, index_ref);
}

const Attributes = struct {
    attributes: std.AutoHashMapUnmanaged(u64, u64),

    pub fn jsonStringify(self: Attributes, jws: anytype) !void {
        try jws.beginArray();
        var iter = self.attributes.iterator();
        while (iter.next()) |entry| {
            try jws.beginArray();
            try jws.write(entry.key_ptr.*);
            try jws.write(entry.value_ptr.*);
            try jws.endArray();
        }
        try jws.endArray();
    }

    pub fn msgpackWrite(self: Attributes, packer: anytype) !void {
        try packer.writeMapHeader(self.attributes.count());
        var iter = self.attributes.iterator();
        while (iter.next()) |entry| {
            try packer.write(@TypeOf(entry.key_ptr.*), entry.key_ptr.*);
            try packer.write(@TypeOf(entry.value_ptr.*), entry.value_ptr.*);
        }
    }
};

const GetIndexResponse = struct {
    version: u64,
    attributes: Attributes,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

fn handleGetIndex(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_ref = try getIndex(ctx, req, res, true) orelse return;
    defer releaseIndex(ctx, index_ref);

    const info = try index_ref.index.getInfo(req.arena);
    const response = GetIndexResponse{
        .version = info.version,
        .attributes = .{
            .attributes = info.attributes,
        },
    };
    return writeResponse(response, req, res);
}

const EmptyResponse = struct {};

fn handlePutIndex(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = req.param("index") orelse return;

    try ctx.indexes.createIndex(index_name);

    return writeResponse(EmptyResponse{}, req, res);
}

fn handleDeleteIndex(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = req.param("index") orelse return;

    try ctx.indexes.deleteIndex(index_name);

    return writeResponse(EmptyResponse{}, req, res);
}

fn handlePing(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    _ = ctx;
    _ = req;

    try res.writer().writeAll("pong\n");
}

fn handleMetrics(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    _ = ctx;
    _ = req;

    const writer = res.writer();

    try metrics.writeMetrics(writer);
    try httpz.writeMetrics(writer);
}
