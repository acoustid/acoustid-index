const std = @import("std");
const httpz = @import("httpz");
const json = std.json;
const log = std.log.scoped(.server);

const msgpack = @import("msgpack");

const MultiIndex = @import("MultiIndex.zig");
const IndexReader = @import("IndexReader.zig");
const common = @import("common.zig");
const SearchResults = common.SearchResults;
const DocInfo = common.DocInfo;
const Change = @import("change.zig").Change;
const Deadline = @import("utils/Deadline.zig");
const Metadata = @import("Metadata.zig");

// Types needed for vtable interface
const GetIndexInfoResult = struct {
    version: u64,
    metadata: Metadata,
    stats: IndexReader.Stats,
};

const GetSegmentInfoResult = struct {
    kind: []const u8,
    version: u64,
    merges: u64,
    min_doc_id: u32,
    max_doc_id: u32,
};

const metrics = @import("metrics.zig");

// Generic index manager interface using vtable pattern
const IndexManagerVTable = struct {
    search: *const fn (ptr: *anyopaque, index_name: []const u8, hashes: []u32, results: *SearchResults, deadline: Deadline) anyerror!void,
    update: *const fn (ptr: *anyopaque, index_name: []const u8, changes: []const Change, metadata: ?Metadata, expected_version: ?u64) anyerror!u64,
    getFingerprintInfo: *const fn (ptr: *anyopaque, index_name: []const u8, id: u32) anyerror!?DocInfo,
    getIndexInfo: *const fn (ptr: *anyopaque, index_name: []const u8, arena: std.mem.Allocator) anyerror!GetIndexInfoResult,
    createIndexAndGetInfo: *const fn (ptr: *anyopaque, index_name: []const u8) anyerror!u64,
    deleteIndex: *const fn (ptr: *anyopaque, index_name: []const u8) anyerror!void,
    indexExists: *const fn (ptr: *anyopaque, index_name: []const u8) bool,
    getSegmentsInfo: *const fn (ptr: *anyopaque, index_name: []const u8, arena: std.mem.Allocator) anyerror![]GetSegmentInfoResult,
    buildSnapshot: *const fn (ptr: *anyopaque, index_name: []const u8, writer: std.io.AnyWriter, arena: std.mem.Allocator) anyerror!void,
};

const IndexManager = struct {
    ptr: *anyopaque,
    vtable: *const IndexManagerVTable,

    pub fn search(self: IndexManager, index_name: []const u8, hashes: []u32, results: *SearchResults, deadline: Deadline) !void {
        return self.vtable.search(self.ptr, index_name, hashes, results, deadline);
    }

    pub fn update(self: IndexManager, index_name: []const u8, changes: []const Change, metadata: ?Metadata, expected_version: ?u64) !u64 {
        return self.vtable.update(self.ptr, index_name, changes, metadata, expected_version);
    }

    pub fn getFingerprintInfo(self: IndexManager, index_name: []const u8, id: u32) !?DocInfo {
        return self.vtable.getFingerprintInfo(self.ptr, index_name, id);
    }

    pub fn getIndexInfo(self: IndexManager, index_name: []const u8, arena: std.mem.Allocator) !GetIndexInfoResult {
        return self.vtable.getIndexInfo(self.ptr, index_name, arena);
    }

    pub fn createIndexAndGetInfo(self: IndexManager, index_name: []const u8) !u64 {
        return self.vtable.createIndexAndGetInfo(self.ptr, index_name);
    }

    pub fn deleteIndex(self: IndexManager, index_name: []const u8) !void {
        return self.vtable.deleteIndex(self.ptr, index_name);
    }

    pub fn indexExists(self: IndexManager, index_name: []const u8) bool {
        return self.vtable.indexExists(self.ptr, index_name);
    }

    pub fn getSegmentsInfo(self: IndexManager, index_name: []const u8, arena: std.mem.Allocator) ![]GetSegmentInfoResult {
        return self.vtable.getSegmentsInfo(self.ptr, index_name, arena);
    }

    pub fn buildSnapshot(self: IndexManager, index_name: []const u8, writer: std.io.AnyWriter, arena: std.mem.Allocator) !void {
        return self.vtable.buildSnapshot(self.ptr, index_name, writer, arena);
    }
};

const Context = struct {
    indexes: IndexManager,

    pub fn notFound(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
        return handleNotFound(ctx, req, res);
    }

    pub fn uncaughtError(ctx: *Context, req: *httpz.Request, res: *httpz.Response, err: anyerror) void {
        return handleError(ctx, req, res, err);
    }
};

// Generic global server storage - must be set before signal handlers
var global_server_storage: ?struct {
    ptr: *anyopaque,
    stop_fn: *const fn (*anyopaque) void,
} = null;

fn shutdown(_: c_int) callconv(.C) void {
    if (global_server_storage) |gs| {
        log.info("stopping", .{});
        const temp_ptr = gs.ptr;
        const temp_fn = gs.stop_fn;
        global_server_storage = null;
        temp_fn(temp_ptr);
    }
}

fn installSignalHandlers(server: anytype) void {
    const ServerType = @TypeOf(server.*);
    const serverStopFn = struct {
        fn stopFn(ptr: *anyopaque) void {
            const s: *ServerType = @ptrCast(@alignCast(ptr));
            s.stop();
        }
    }.stopFn;
    
    global_server_storage = .{
        .ptr = server,
        .stop_fn = serverStopFn,
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

// Helper to create IndexManager from any type that implements the required interface
pub fn createIndexManager(comptime T: type, instance: *T) IndexManager {
    const vtable = IndexManagerVTable{
        .search = struct {
            fn search(ptr: *anyopaque, index_name: []const u8, hashes: []u32, results: *SearchResults, deadline: Deadline) anyerror!void {
                const self: *T = @ptrCast(@alignCast(ptr));
                return self.search(index_name, hashes, results, deadline);
            }
        }.search,
        .update = struct {
            fn update(ptr: *anyopaque, index_name: []const u8, changes: []const Change, metadata: ?Metadata, expected_version: ?u64) anyerror!u64 {
                const self: *T = @ptrCast(@alignCast(ptr));
                return self.update(index_name, changes, metadata, expected_version);
            }
        }.update,
        .getFingerprintInfo = struct {
            fn getFingerprintInfo(ptr: *anyopaque, index_name: []const u8, id: u32) anyerror!?DocInfo {
                const self: *T = @ptrCast(@alignCast(ptr));
                return self.getFingerprintInfo(index_name, id);
            }
        }.getFingerprintInfo,
        .getIndexInfo = struct {
            fn getIndexInfo(ptr: *anyopaque, index_name: []const u8, arena: std.mem.Allocator) anyerror!GetIndexInfoResult {
                const self: *T = @ptrCast(@alignCast(ptr));
                const result = try self.getIndexInfo(index_name, arena);
                return GetIndexInfoResult{
                    .version = result.version,
                    .metadata = result.metadata,
                    .stats = result.stats,
                };
            }
        }.getIndexInfo,
        .createIndexAndGetInfo = struct {
            fn createIndexAndGetInfo(ptr: *anyopaque, index_name: []const u8) anyerror!u64 {
                const self: *T = @ptrCast(@alignCast(ptr));
                return self.createIndexAndGetInfo(index_name);
            }
        }.createIndexAndGetInfo,
        .deleteIndex = struct {
            fn deleteIndex(ptr: *anyopaque, index_name: []const u8) anyerror!void {
                const self: *T = @ptrCast(@alignCast(ptr));
                return self.deleteIndex(index_name);
            }
        }.deleteIndex,
        .indexExists = struct {
            fn indexExists(ptr: *anyopaque, index_name: []const u8) bool {
                const self: *T = @ptrCast(@alignCast(ptr));
                return self.indexExists(index_name);
            }
        }.indexExists,
        .getSegmentsInfo = struct {
            fn getSegmentsInfo(ptr: *anyopaque, index_name: []const u8, arena: std.mem.Allocator) anyerror![]GetSegmentInfoResult {
                const self: *T = @ptrCast(@alignCast(ptr));
                const results = try self.getSegmentsInfo(index_name, arena);
                const converted = try arena.alloc(GetSegmentInfoResult, results.len);
                for (results, 0..) |r, i| {
                    converted[i] = GetSegmentInfoResult{
                        .kind = r.kind,
                        .version = r.version,
                        .merges = r.merges,
                        .min_doc_id = r.min_doc_id,
                        .max_doc_id = r.max_doc_id,
                    };
                }
                return converted;
            }
        }.getSegmentsInfo,
        .buildSnapshot = struct {
            fn buildSnapshot(ptr: *anyopaque, index_name: []const u8, writer: std.io.AnyWriter, arena: std.mem.Allocator) anyerror!void {
                const self: *T = @ptrCast(@alignCast(ptr));
                return self.buildSnapshot(index_name, writer, arena);
            }
        }.buildSnapshot,
    };
    
    return IndexManager{
        .ptr = instance,
        .vtable = &vtable,
    };
}

pub fn run(allocator: std.mem.Allocator, indexes: anytype, address: []const u8, port: u16, threads: u16) !void {
    const T = @TypeOf(indexes.*);
    const manager = createIndexManager(T, indexes);
    var ctx = Context{ .indexes = manager };

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

    const Server = httpz.Server(*Context);
    var server = try Server.init(allocator, config, &ctx);
    defer server.deinit();

    installSignalHandlers(&server);

    var router = try server.router(.{});

    // Monitoring API
    router.get("/_metrics", handleMetrics, .{});
    router.get("/_health", handleHealth, .{});
    router.get("/:index/_health", handleIndexHealth, .{});

    // Search API
    router.post("/:index/_search", handleSearch, .{});

    // Bulk API
    router.post("/:index/_update", handleUpdate, .{});

    // Fingerprint API
    router.head("/:index/:id", handleHeadFingerprint, .{});
    router.get("/:index/:id", handleGetFingerprint, .{});
    router.put("/:index/:id", handlePutFingerprint, .{});
    router.delete("/:index/:id", handleDeleteFingerprint, .{});

    // Index API
    router.head("/:index", handleHeadIndex, .{});
    router.get("/:index", handleGetIndex, .{});
    router.put("/:index", handlePutIndex, .{});
    router.delete("/:index", handleDeleteIndex, .{});
    router.get("/:index/_segments", handleGetSegments, .{});
    router.get("/:index/_snapshot", handleSnapshot, .{});

    log.info("listening on {s}:{d}", .{ address, port });
    try server.listen();
}

const default_search_timeout = 500;
const max_search_timeout = 10000;

const default_search_limit = 40;
const min_search_limit = 1;
const max_search_limit = 100;

const SearchRequestJSON = struct {
    query: []u32,
    timeout: u32 = default_search_timeout,
    limit: u32 = default_search_limit,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

const SearchResultJSON = struct {
    id: u32,
    score: u32,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

const SearchResultsJSON = struct {
    results: []SearchResultJSON,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

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

fn handleNotFound(_: anytype, req: *httpz.Request, res: *httpz.Response) !void {
    try writeErrorResponse(404, error.NotFound, req, res);
}

fn handleError(_: anytype, req: *httpz.Request, res: *httpz.Response, err: anyerror) void {
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

fn handleSearch(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const start_time = std.time.milliTimestamp();
    defer metrics.searchDuration(std.time.milliTimestamp() - start_time);

    const body = try getRequestBody(SearchRequestJSON, req, res) orelse return;

    const index_name = try getIndexName(req, res, true) orelse return;

    const limit = @max(@min(body.limit, max_search_limit), min_search_limit);

    var timeout = body.timeout;
    if (timeout > max_search_timeout) {
        timeout = max_search_timeout;
    }
    const deadline = Deadline.init(timeout);

    metrics.search();

    var collector = SearchResults.init(req.arena, .{
        .max_results = limit,
        .min_score = @intCast((body.query.len + 19) / 20),
        .min_score_pct = 10,
    });

    ctx.indexes.search(index_name, body.query, &collector, deadline) catch |err| {
        if (err == error.IndexNotFound) {
            try writeErrorResponse(404, err, req, res);
            return;
        }
        return err;
    };

    const results = collector.getResults();

    if (results.len == 0) {
        metrics.searchMiss();
    } else {
        metrics.searchHit();
    }

    var results_json = SearchResultsJSON{
        .results = try req.arena.alloc(SearchResultJSON, results.len),
    };
    for (results, 0..) |r, i| {
        results_json.results[i] = SearchResultJSON{ .id = r.id, .score = r.score };
    }
    return writeResponse(results_json, req, res);
}

const UpdateRequestJSON = struct {
    changes: []Change,
    metadata: ?Metadata = null,
    expected_version: ?u64 = null,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

fn handleUpdate(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const body = try getRequestBody(UpdateRequestJSON, req, res) orelse return;

    const index_name = try getIndexName(req, res, true) orelse return;

    metrics.update(body.changes.len);

    const new_version = ctx.indexes.update(index_name, body.changes, body.metadata, body.expected_version) catch |err| {
        if (err == error.IndexNotFound) {
            try writeErrorResponse(404, err, req, res);
            return;
        }
        if (err == error.VersionMismatch) {
            try writeErrorResponse(409, err, req, res);
            return;
        }
        return err;
    };

    return writeResponse(UpdateResponse{ .version = new_version }, req, res);
}

fn handleHeadFingerprint(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = try getIndexName(req, res, false) orelse return;

    const id = try getId(req, res, false) orelse return;
    
    const info = ctx.indexes.getFingerprintInfo(index_name, id) catch |err| {
        if (err == error.IndexNotFound) {
            res.status = 404;
            return;
        }
        return err;
    };

    res.status = if (info == null) 404 else 200;
}

const GetFingerprintResponse = struct {
    version: u64,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

fn handleGetFingerprint(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = try getIndexName(req, res, true) orelse return;

    const id = try getId(req, res, true) orelse return;
    
    const info = ctx.indexes.getFingerprintInfo(index_name, id) catch |err| {
        if (err == error.IndexNotFound) {
            try writeErrorResponse(404, err, req, res);
            return;
        }
        return err;
    } orelse {
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

fn handlePutFingerprint(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const body = try getRequestBody(PutFingerprintRequest, req, res) orelse return;

    const index_name = try getIndexName(req, res, true) orelse return;

    const id = try getId(req, res, true) orelse return;
    const change: Change = .{ .insert = .{
        .id = id,
        .hashes = body.hashes,
    } };

    metrics.update(1);

    _ = ctx.indexes.update(index_name, &[_]Change{change}, null, null) catch |err| {
        if (err == error.IndexNotFound) {
            try writeErrorResponse(404, err, req, res);
            return;
        }
        return err;
    };

    return writeResponse(EmptyResponse{}, req, res);
}

fn handleDeleteFingerprint(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = try getIndexName(req, res, true) orelse return;

    const id = try getId(req, res, true) orelse return;
    const change: Change = .{ .delete = .{
        .id = id,
    } };

    metrics.update(1);

    _ = ctx.indexes.update(index_name, &[_]Change{change}, null, null) catch |err| {
        if (err == error.IndexNotFound) {
            try writeErrorResponse(404, err, req, res);
            return;
        }
        return err;
    };

    return writeResponse(EmptyResponse{}, req, res);
}

const GetIndexResponse = struct {
    version: u64,
    metadata: Metadata,
    stats: IndexReader.Stats,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

fn handleGetIndex(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = try getIndexName(req, res, true) orelse return;

    const info = ctx.indexes.getIndexInfo(index_name, req.arena) catch |err| {
        if (err == error.IndexNotFound) {
            try writeErrorResponse(404, err, req, res);
            return;
        }
        return err;
    };

    const response = GetIndexResponse{
        .version = info.version,
        .metadata = info.metadata,
        .stats = info.stats,
    };
    return writeResponse(response, req, res);
}

const EmptyResponse = struct {};

const UpdateResponse = struct {
    version: u64,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

const CreateIndexRequest = struct {
    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

const CreateIndexResponse = struct {
    version: u64,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

fn handlePutIndex(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = try getIndexName(req, res, true) orelse return;

    const version = try ctx.indexes.createIndexAndGetInfo(index_name);
    
    const response = CreateIndexResponse{
        .version = version,
    };

    return writeResponse(response, req, res);
}

fn handleDeleteIndex(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = try getIndexName(req, res, true) orelse return;

    try ctx.indexes.deleteIndex(index_name);

    return writeResponse(EmptyResponse{}, req, res);
}

fn handleHeadIndex(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = try getIndexName(req, res, false) orelse return;

    if (!ctx.indexes.indexExists(index_name)) {
        res.status = 404;
    }
}

fn handleIndexHealth(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = try getIndexName(req, res, false) orelse {
        try res.writer().writeAll("Bad Request\n");
        return;
    };

    if (!ctx.indexes.indexExists(index_name)) {
        res.status = 404;
        try res.writer().writeAll("Not Found\n");
        return;
    }

    try res.writer().writeAll("OK\n");
}


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

fn handleGetSegments(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = try getIndexName(req, res, true) orelse return;

    const segments_info = ctx.indexes.getSegmentsInfo(index_name, req.arena) catch |err| {
        if (err == error.IndexNotFound) {
            try writeErrorResponse(404, err, req, res);
            return;
        }
        return err;
    };

    const segments = try req.arena.alloc(GetSegmentResponse, segments_info.len);
    for (segments_info, 0..) |info, i| {
        segments[i] = .{
            .kind = info.kind,
            .version = info.version,
            .merges = info.merges,
            .min_doc_id = info.min_doc_id,
            .max_doc_id = info.max_doc_id,
        };
    }

    return writeResponse(GetSegmentsResponse{ .segments = segments }, req, res);
}

fn handleHealth(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    _ = ctx;
    _ = req;

    try res.writer().writeAll("OK\n");
}

fn handleSnapshot(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index_name = try getIndexName(req, res, true) orelse return;

    // Set response headers for tar download
    res.header("content-type", "application/x-tar");
    res.header("content-disposition", "attachment; filename=\"index_snapshot.tar\"");

    // Build snapshot using MultiIndex method
    ctx.indexes.buildSnapshot(index_name, res.writer().any(), req.arena) catch |err| {
        if (err == error.IndexNotFound) {
            try writeErrorResponse(404, err, req, res);
            return;
        }
        return err;
    };
}

fn handleMetrics(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    _ = ctx;
    _ = req;

    const writer = res.writer();

    try metrics.writeMetrics(writer);
    try httpz.writeMetrics(writer);
}
