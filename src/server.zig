const std = @import("std");
const httpz = @import("httpz");
const json = std.json;
const log = std.log.scoped(.server);
const assert = std.debug.assert;

const msgpack = @import("msgpack");

const MultiIndex = @import("MultiIndex.zig");
const Index = @import("Index.zig");
const IndexReader = @import("IndexReader.zig");
const common = @import("common.zig");
const SearchResults = common.SearchResults;
const Change = @import("change.zig").Change;
const Deadline = @import("utils/Deadline.zig");
const snapshot = @import("snapshot.zig");
const Metadata = @import("Metadata.zig");
const api = @import("api.zig");

const metrics = @import("metrics.zig");

fn Context(comptime T: type) type {
    return struct {
        indexes: *T,

        const Self = @This();

        pub fn notFound(ctx: *Self, req: *httpz.Request, res: *httpz.Response) !void {
            return handleNotFound(T, ctx, req, res);
        }

        pub fn uncaughtError(ctx: *Self, req: *httpz.Request, res: *httpz.Response, err: anyerror) void {
            return handleError(T, ctx, req, res, err);
        }
    };
}

fn Server(comptime T: type) type {
    return httpz.Server(*Context(T));
}

// Interface for server shutdown
const ServerStopper = struct {
    ptr: *anyopaque,
    stopFn: *const fn (*anyopaque) void,

    pub fn stop(self: ServerStopper) void {
        self.stopFn(self.ptr);
    }
};

var global_server: ?ServerStopper = null;

fn shutdown(_: c_int) callconv(.C) void {
    if (global_server) |server| {
        log.info("stopping", .{});
        server.stop();
        global_server = null;
    }
}

fn installSignalHandlers(comptime T: type, server: *Server(T)) void {
    const ServerType = Server(T);
    global_server = .{
        .ptr = server,
        .stopFn = struct {
            fn stop(ptr: *anyopaque) void {
                const s: *ServerType = @ptrCast(@alignCast(ptr));
                s.stop();
            }
        }.stop,
    };

    std.posix.sigaction(std.posix.SIG.INT, &.{
        .handler = .{ .handler = shutdown },
        .mask = std.posix.empty_sigset,
        .flags = 0,
    }, null);

    std.posix.sigaction(std.posix.SIG.TERM, &.{
        .handler = .{ .handler = shutdown },
        .mask = std.posix.empty_sigset,
        .flags = 0,
    }, null);
}

// Generic wrapper generator - creates a struct with the wrapper function
fn HandlerWrapper(comptime T: type, comptime handler_fn: anytype) type {
    const ContextType = Context(T);
    return struct {
        pub fn wrapper(context: *ContextType, req: *httpz.Request, res: *httpz.Response) !void {
            return handler_fn(T, context, req, res);
        }
    };
}

pub fn run(comptime T: type, allocator: std.mem.Allocator, indexes: *T, address: []const u8, port: u16, threads: u16) !void {
    const ContextType = Context(T);
    var ctx = ContextType{ .indexes = indexes };

    const config = httpz.Config{
        .address = address,
        .port = port,
        .thread_pool = .{
            .count = threads,
        },
        .timeout = .{
            .request = null,
            .keepalive = 300,
        },
        .request = .{
            .max_body_size = 16 * 1024 * 1024,
        },
    };

    const ServerType = Server(T);
    var server = try ServerType.init(allocator, config, &ctx);
    defer server.deinit();

    installSignalHandlers(T, &server);

    var router = try server.router(.{});

    // Monitoring API
    router.get("/_metrics", HandlerWrapper(T, handleMetrics).wrapper, .{});
    router.get("/_health", HandlerWrapper(T, handleHealth).wrapper, .{});
    router.get("/:index/_health", HandlerWrapper(T, handleIndexHealth).wrapper, .{});

    // Search API
    router.post("/:index/_search", HandlerWrapper(T, handleSearch).wrapper, .{});

    // Bulk API
    router.post("/:index/_update", HandlerWrapper(T, handleUpdate).wrapper, .{});

    // Fingerprint API
    router.head("/:index/:id", HandlerWrapper(T, handleHeadFingerprint).wrapper, .{});
    router.get("/:index/:id", HandlerWrapper(T, handleGetFingerprint).wrapper, .{});
    router.put("/:index/:id", HandlerWrapper(T, handlePutFingerprint).wrapper, .{});
    router.delete("/:index/:id", HandlerWrapper(T, handleDeleteFingerprint).wrapper, .{});

    // Index API
    router.head("/:index", HandlerWrapper(T, handleHeadIndex).wrapper, .{});
    router.get("/:index", HandlerWrapper(T, handleGetIndex).wrapper, .{});
    router.put("/:index", HandlerWrapper(T, handlePutIndex).wrapper, .{});
    router.delete("/:index", HandlerWrapper(T, handleDeleteIndex).wrapper, .{});
    router.get("/:index/_segments", HandlerWrapper(T, handleGetSegments).wrapper, .{});
    router.get("/:index/_snapshot", HandlerWrapper(T, handleSnapshot).wrapper, .{});

    log.info("listening on {s}:{d}", .{ address, port });
    try server.listen();
}

fn getId(req: *httpz.Request, res: *httpz.Response, send_body: bool) !?u32 {
    const id_str = req.param("id") orelse {
        log.warn("missing id parameter", .{});
        if (send_body) {
            try writeErrorResponse(400, error.MissingId, req, res);
        } else {
            res.status = 400;
        }
        return null;
    };
    return std.fmt.parseInt(u32, id_str, 10) catch |err| {
        log.warn("invalid id parameter: {}", .{err});
        if (send_body) {
            try writeErrorResponse(400, err, req, res);
        } else {
            res.status = 400;
        }
        return null;
    };
}

fn getIndexName(req: *httpz.Request, res: *httpz.Response, send_body: bool) !?[]const u8 {
    const index_name = req.param("index") orelse {
        log.warn("missing index parameter", .{});
        if (send_body) {
            try writeErrorResponse(400, error.MissingIndexName, req, res);
        } else {
            res.status = 400;
        }
        return null;
    };
    return index_name;
}

fn getIndex(comptime T: type, ctx: *Context(T), req: *httpz.Request, res: *httpz.Response, send_body: bool) !?*Index {
    const index_name = req.param("index") orelse {
        log.warn("missing index parameter", .{});
        if (send_body) {
            try writeErrorResponse(400, error.MissingIndexName, req, res);
        } else {
            res.status = 400;
        }
        return null;
    };
    const index = ctx.indexes.getIndex(index_name) catch |err| {
        log.warn("error during getIndex: {}", .{err});
        if (err == error.IndexNotFound) {
            if (send_body) {
                try writeErrorResponse(404, err, req, res);
            } else {
                res.status = 404;
            }
            return null;
        }
        return err;
    };
    return index;
}

fn releaseIndex(comptime T: type, ctx: *Context(T), index: *Index) void {
    ctx.indexes.releaseIndex(index);
}

const ContentType = enum {
    json,
    msgpack,
};

const jsonContentTypeName = "application/json";
const msgpackContentTypeName = "application/vnd.msgpack";

fn getDefaultContentType(req: *httpz.Request) ContentType {
    if (req.body() != null) {
        return .msgpack;
    } else {
        return .json;
    }
}

fn parseContentTypeHeader(req: *httpz.Request) !ContentType {
    if (req.header("content-type")) |content_type| {
        if (std.mem.eql(u8, content_type, jsonContentTypeName)) {
            return .json;
        } else if (std.mem.eql(u8, content_type, msgpackContentTypeName)) {
            return .msgpack;
        }
        return error.InvalidContentType;
    }
    return getDefaultContentType(req);
}

fn parseAcceptHeader(req: *httpz.Request) ContentType {
    if (req.header("accept")) |accept_header| {
        if (std.mem.eql(u8, accept_header, jsonContentTypeName)) {
            return .json;
        } else if (std.mem.eql(u8, accept_header, msgpackContentTypeName)) {
            return .msgpack;
        }
    }
    return parseContentTypeHeader(req) catch getDefaultContentType(req);
}

fn writeResponse(value: anytype, req: *httpz.Request, res: *httpz.Response) !void {
    const content_type = parseAcceptHeader(req);
    switch (content_type) {
        .json => try res.json(value, .{}),
        .msgpack => {
            res.header("content-type", msgpackContentTypeName);
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

fn handleNotFound(comptime T: type, _: *Context(T), req: *httpz.Request, res: *httpz.Response) !void {
    try writeErrorResponse(404, error.NotFound, req, res);
}

fn handleError(comptime T: type, _: *Context(T), req: *httpz.Request, res: *httpz.Response, err: anyerror) void {
    switch (err) {
        error.IndexNotReady => {
            writeErrorResponse(503, err, req, res) catch {
                res.status = 503;
                res.body = "not ready yet";
            };
        },
        else => {
            log.err("unhandled error in {s}: {any}", .{ req.url.raw, err });
            writeErrorResponse(500, err, req, res) catch {
                res.status = 500;
                res.body = "internal error";
            };
        },
    }
}

fn writeErrorResponse(status: u16, err: anyerror, req: *httpz.Request, res: *httpz.Response) !void {
    res.status = status;
    try writeResponse(ErrorResponse{ .@"error" = @errorName(err) }, req, res);
}

fn getRequestBody(comptime T: type, req: *httpz.Request, res: *httpz.Response) !?T {
    const content = req.body() orelse {
        log.warn("no body", .{});
        try writeErrorResponse(400, error.NoContent, req, res);
        return null;
    };

    const content_type = parseContentTypeHeader(req) catch {
        try writeErrorResponse(415, error.UnsupportedContentType, req, res);
        return null;
    };

    switch (content_type) {
        .json => {
            return json.parseFromSliceLeaky(T, req.arena, content, .{}) catch |err| {
                log.warn("json error: {}", .{err});
                try writeErrorResponse(400, err, req, res);
                return null;
            };
        },
        .msgpack => {
            return msgpack.decodeFromSliceLeaky(T, req.arena, content) catch |err| {
                log.warn("msgpack error: {}", .{err});
                try writeErrorResponse(400, err, req, res);
                return null;
            };
        },
    }

    unreachable;
}

fn handleSearch(comptime T: type, ctx: *Context(T), req: *httpz.Request, res: *httpz.Response) !void {
    const start_time = std.time.milliTimestamp();
    defer metrics.searchDuration(std.time.milliTimestamp() - start_time);

    const index_name = try getIndexName(req, res, true) orelse return;
    const body = try getRequestBody(api.SearchRequest, req, res) orelse return;

    const response = ctx.indexes.search(req.arena, index_name, body) catch |err| {
        if (err == error.IndexNotFound) {
            try writeErrorResponse(404, err, req, res);
            return;
        }
        return err;
    };
    comptime assert(@TypeOf(response) == api.SearchResponse);

    return writeResponse(response, req, res);
}

fn handleUpdate(comptime T: type, ctx: *Context(T), req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = try getIndexName(req, res, true) orelse return;
    const body = try getRequestBody(api.UpdateRequest, req, res) orelse return;

    const response = ctx.indexes.update(req.arena, index_name, body) catch |err| {
        if (err == error.VersionMismatch) {
            return writeErrorResponse(409, err, req, res);
        }
        if (err == error.IndexNotFound) {
            return writeErrorResponse(404, err, req, res);
        }
        return err;
    };
    comptime assert(@TypeOf(response) == api.UpdateResponse);

    return writeResponse(response, req, res);
}

fn handleHeadFingerprint(comptime T: type, ctx: *Context(T), req: *httpz.Request, res: *httpz.Response) !void {
    const index = try getIndex(T, ctx, req, res, false) orelse return;
    defer releaseIndex(T, ctx, index);

    var index_reader = try index.acquireReader();
    defer index.releaseReader(&index_reader);

    const id = try getId(req, res, false) orelse return;
    const info = try index_reader.getDocInfo(id);

    res.status = if (info == null) 404 else 200;
}

const GetFingerprintResponse = struct {
    version: u64,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

fn handleGetFingerprint(comptime T: type, ctx: *Context(T), req: *httpz.Request, res: *httpz.Response) !void {
    const index = try getIndex(T, ctx, req, res, true) orelse return;
    defer releaseIndex(T, ctx, index);

    var index_reader = try index.acquireReader();
    defer index.releaseReader(&index_reader);

    const id = try getId(req, res, true) orelse return;
    const info = try index_reader.getDocInfo(id) orelse {
        return writeErrorResponse(404, error.FingerprintNotFound, req, res);
    };

    return writeResponse(GetFingerprintResponse{ .version = info.version }, req, res);
}

const PutFingerprintRequest = struct {
    hashes: []u32,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

fn handlePutFingerprint(comptime T: type, ctx: *Context(T), req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = try getIndexName(req, res, true) orelse return;
    const id = try getId(req, res, true) orelse return;
    const body = try getRequestBody(PutFingerprintRequest, req, res) orelse return;

    var changes: [1]Change = .{.{ .insert = .{
        .id = id,
        .hashes = body.hashes,
    } }};

    const update_request = api.UpdateRequest{
        .changes = &changes,
        .metadata = null,
        .expected_version = null,
    };

    const response = ctx.indexes.update(req.arena, index_name, update_request) catch |err| {
        if (err == error.VersionMismatch) {
            return writeErrorResponse(409, err, req, res);
        }
        if (err == error.IndexNotFound) {
            return writeErrorResponse(404, err, req, res);
        }
        return err;
    };
    comptime assert(@TypeOf(response) == api.UpdateResponse);

    return writeResponse(EmptyResponse{}, req, res);
}

fn handleDeleteFingerprint(comptime T: type, ctx: *Context(T), req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = try getIndexName(req, res, true) orelse return;
    const id = try getId(req, res, true) orelse return;

    var changes: [1]Change = .{.{ .delete = .{
        .id = id,
    } }};

    const update_request = api.UpdateRequest{
        .changes = &changes,
        .metadata = null,
        .expected_version = null,
    };

    const response = ctx.indexes.update(req.arena, index_name, update_request) catch |err| {
        if (err == error.VersionMismatch) {
            return writeErrorResponse(409, err, req, res);
        }
        if (err == error.IndexNotFound) {
            return writeErrorResponse(404, err, req, res);
        }
        return err;
    };
    comptime assert(@TypeOf(response) == api.UpdateResponse);

    return writeResponse(EmptyResponse{}, req, res);
}


fn handleGetIndex(comptime T: type, ctx: *Context(T), req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = try getIndexName(req, res, true) orelse return;

    const response = ctx.indexes.getIndexInfo(req.arena, index_name) catch |err| {
        if (err == error.IndexNotFound) {
            try writeErrorResponse(404, err, req, res);
            return;
        }
        return err;
    };
    comptime assert(@TypeOf(response) == api.GetIndexInfoResponse);

    return writeResponse(response, req, res);
}

const EmptyResponse = struct {};

const CreateIndexRequest = struct {
    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};


fn handlePutIndex(comptime T: type, ctx: *Context(T), req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = try getIndexName(req, res, true) orelse return;

    const response = ctx.indexes.createIndex(req.arena, index_name) catch |err| {
        if (err == error.InvalidIndexName) {
            try writeErrorResponse(400, err, req, res);
            return;
        }
        return err;
    };
    comptime assert(@TypeOf(response) == api.CreateIndexResponse);

    return writeResponse(response, req, res);
}

fn handleDeleteIndex(comptime T: type, ctx: *Context(T), req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = try getIndexName(req, res, true) orelse return;

    ctx.indexes.deleteIndex(index_name) catch |err| {
        if (err == error.InvalidIndexName) {
            try writeErrorResponse(400, err, req, res);
            return;
        }
        if (err == error.IndexNotFound) {
            try writeErrorResponse(404, err, req, res);
            return;
        }
        if (err == error.DeleteTimeout or err == error.IndexAlreadyBeingDeleted) {
            try writeErrorResponse(409, err, req, res);
            return;
        }
        return err;
    };

    return writeResponse(EmptyResponse{}, req, res);
}

fn handleHeadIndex(comptime T: type, ctx: *Context(T), req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = try getIndexName(req, res, false) orelse return;

    ctx.indexes.checkIndexExists(index_name) catch |err| {
        if (err == error.IndexNotFound) {
            res.status = 404;
            return;
        }
        return err;
    };

    res.status = 200;
}

fn handleIndexHealth(comptime T: type, ctx: *Context(T), req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = try getIndexName(req, res, false) orelse return;

    ctx.indexes.checkIndexExists(index_name) catch |err| {
        if (err == error.IndexNotFound) {
            res.status = 404;
            return;
        }
        return err;
    };

    try res.writer().writeAll("OK\n");
}

const MEMORY_SEGMENT = "memory";
const FILE_SEGMENT = "file";

pub const GetSegmentResponse = struct {
    kind: []const u8,
    version: u64,
    merges: u64,
    min_doc_id: u32,
    max_doc_id: u32,
};

pub const GetSegmentsResponse = struct {
    segments: []GetSegmentResponse,
};

fn handleGetSegments(comptime T: type, ctx: *Context(T), req: *httpz.Request, res: *httpz.Response) !void {
    const index = try getIndex(T, ctx, req, res, true) orelse return;
    defer releaseIndex(T, ctx, index);

    var reader = try index.acquireReader();
    defer index.releaseReader(&reader);

    const num_segments = reader.file_segments.value.count() + reader.memory_segments.value.count();
    const segments = try req.arena.alloc(GetSegmentResponse, num_segments);

    var i: usize = 0;

    for (reader.file_segments.value.nodes.items) |segment| {
        segments[i] = .{
            .kind = FILE_SEGMENT,
            .version = segment.value.info.version,
            .merges = segment.value.info.merges,
            .min_doc_id = segment.value.min_doc_id,
            .max_doc_id = segment.value.max_doc_id,
        };
        i += 1;
    }

    for (reader.memory_segments.value.nodes.items) |segment| {
        segments[i] = .{
            .kind = MEMORY_SEGMENT,
            .version = segment.value.info.version,
            .merges = segment.value.info.merges,
            .min_doc_id = segment.value.min_doc_id,
            .max_doc_id = segment.value.max_doc_id,
        };
        i += 1;
    }

    return writeResponse(GetSegmentsResponse{ .segments = segments }, req, res);
}

fn handleHealth(comptime T: type, ctx: *Context(T), req: *httpz.Request, res: *httpz.Response) !void {
    _ = ctx;
    _ = req;

    try res.writer().writeAll("OK\n");
}

fn handleSnapshot(comptime T: type, ctx: *Context(T), req: *httpz.Request, res: *httpz.Response) !void {
    const index = try getIndex(T, ctx, req, res, true) orelse return;
    defer releaseIndex(T, ctx, index);

    // Set response headers for tar download
    res.header("content-type", "application/x-tar");
    res.header("content-disposition", "attachment; filename=\"index_snapshot.tar\"");

    // Build snapshot using the dedicated module
    try snapshot.buildSnapshot(res.writer().any(), index, req.arena);
}

fn handleMetrics(comptime T: type, ctx: *Context(T), req: *httpz.Request, res: *httpz.Response) !void {
    _ = ctx;
    _ = req;

    const writer = res.writer();

    try metrics.writeMetrics(writer);
    try httpz.writeMetrics(writer);
}
