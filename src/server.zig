const std = @import("std");
const httpz = @import("httpz");
const json = std.json;
const log = std.log.scoped(.server);

const msgpack = @import("msgpack");

const MultiIndex = @import("MultiIndex.zig");
const Index = @import("Index.zig");
const IndexReader = @import("IndexReader.zig");
const common = @import("common.zig");
const SearchResults = common.SearchResults;
const Change = @import("change.zig").Change;
const Deadline = @import("utils/Deadline.zig");
const snapshot = @import("snapshot.zig");

const metrics = @import("metrics.zig");

const Context = struct {
    indexes: *MultiIndex,

    pub fn notFound(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
        return handleNotFound(ctx, req, res);
    }

    pub fn uncaughtError(ctx: *Context, req: *httpz.Request, res: *httpz.Response, err: anyerror) void {
        return handleError(ctx, req, res, err);
    }
};

const Server = httpz.Server(*Context);

var global_server: ?*Server = null;

fn shutdown(_: c_int) callconv(.C) void {
    if (global_server) |server| {
        log.info("stopping", .{});
        global_server = null;
        server.stop();
    }
}

fn installSignalHandlers(server: *Server) void {
    global_server = server;

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

pub fn run(allocator: std.mem.Allocator, indexes: *MultiIndex, address: []const u8, port: u16, threads: u16) !void {
    var ctx = Context{ .indexes = indexes };

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

fn getIndex(ctx: *Context, req: *httpz.Request, res: *httpz.Response, send_body: bool) !?*Index {
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

fn releaseIndex(ctx: *Context, index: *Index) void {
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

fn handleNotFound(_: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    try writeErrorResponse(404, error.NotFound, req, res);
}

fn handleError(_: *Context, req: *httpz.Request, res: *httpz.Response, err: anyerror) void {
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

    const index = try getIndex(ctx, req, res, true) orelse return;
    defer releaseIndex(ctx, index);

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

    try index.search(body.query, &collector, deadline);

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

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

fn handleUpdate(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const body = try getRequestBody(UpdateRequestJSON, req, res) orelse return;

    const index = try getIndex(ctx, req, res, true) orelse return;
    defer releaseIndex(ctx, index);

    metrics.update(body.changes.len);

    try index.update(body.changes);

    return writeResponse(EmptyResponse{}, req, res);
}

fn handleHeadFingerprint(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index = try getIndex(ctx, req, res, false) orelse return;
    defer releaseIndex(ctx, index);

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

fn handleGetFingerprint(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index = try getIndex(ctx, req, res, true) orelse return;
    defer releaseIndex(ctx, index);

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

fn handlePutFingerprint(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const body = try getRequestBody(PutFingerprintRequest, req, res) orelse return;

    const index = try getIndex(ctx, req, res, true) orelse return;
    defer releaseIndex(ctx, index);

    const id = try getId(req, res, true) orelse return;
    const change: Change = .{ .insert = .{
        .id = id,
        .hashes = body.hashes,
    } };

    metrics.update(1);

    try index.update(&[_]Change{change});

    return writeResponse(EmptyResponse{}, req, res);
}

fn handleDeleteFingerprint(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index = try getIndex(ctx, req, res, true) orelse return;
    defer releaseIndex(ctx, index);

    const id = try getId(req, res, true) orelse return;
    const change: Change = .{ .delete = .{
        .id = id,
    } };

    metrics.update(1);

    try index.update(&[_]Change{change});

    return writeResponse(EmptyResponse{}, req, res);
}

const Metadata = struct {
    metadata: std.StringHashMap([]const u8),

    pub fn jsonStringify(self: Metadata, jws: anytype) !void {
        try jws.beginObject();
        var iter = self.metadata.iterator();
        while (iter.next()) |entry| {
            try jws.objectField(entry.key_ptr.*);
            try jws.write(entry.value_ptr.*);
        }
        try jws.endObject();
    }

    pub fn msgpackWrite(self: Metadata, packer: anytype) !void {
        try packer.writeMapHeader(self.metadata.count());
        var iter = self.metadata.iterator();
        while (iter.next()) |entry| {
            try packer.write(entry.key_ptr.*);
            try packer.write(entry.value_ptr.*);
        }
    }
};

const Stats = struct {
    min_document_id: ?u32,
    max_document_id: ?u32,

    pub fn jsonStringify(self: Stats, jws: anytype) !void {
        try jws.beginObject();
        try jws.objectField("min_document_id");
        try jws.write(self.min_document_id);
        try jws.objectField("max_document_id");
        try jws.write(self.max_document_id);
        try jws.endObject();
    }

    pub fn msgpackWrite(self: Stats, packer: anytype) !void {
        try packer.writeMapHeader(2);
        try packer.write("min_document_id");
        try packer.write(self.min_document_id);
        try packer.write("max_document_id");
        try packer.write(self.max_document_id);
    }
};

const GetIndexResponse = struct {
    version: u64,
    segments: usize,
    docs: usize,
    metadata: Metadata,
    stats: Stats,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

fn handleGetIndex(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index = try getIndex(ctx, req, res, true) orelse return;
    defer releaseIndex(ctx, index);

    var index_reader = try index.acquireReader();
    defer index.releaseReader(&index_reader);

    const response = GetIndexResponse{
        .version = index_reader.getVersion(),
        .segments = index_reader.getNumSegments(),
        .docs = index_reader.getNumDocs(),
        .metadata = .{
            .metadata = blk: {
                var managed_metadata = std.StringHashMap([]const u8).init(req.arena);
                const unmanaged_metadata = try index_reader.getMetadata(req.arena);
                var iter = unmanaged_metadata.iterator();
                while (iter.next()) |entry| {
                    try managed_metadata.put(entry.key_ptr.*, entry.value_ptr.*);
                }
                break :blk managed_metadata;
            },
        },
        .stats = Stats{
            .min_document_id = index_reader.getStats().min_document_id,
            .max_document_id = index_reader.getStats().max_document_id,
        },
    };
    return writeResponse(response, req, res);
}

const EmptyResponse = struct {};

const CreateIndexRequest = struct {
    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

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

fn handleHeadIndex(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index = try getIndex(ctx, req, res, false) orelse return;
    defer releaseIndex(ctx, index);
}

fn handleIndexHealth(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index = try getIndex(ctx, req, res, false) orelse return;
    defer releaseIndex(ctx, index);

    try index.checkReady();

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

fn handleGetSegments(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index = try getIndex(ctx, req, res, true) orelse return;
    defer releaseIndex(ctx, index);

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

fn handleHealth(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    _ = ctx;
    _ = req;

    try res.writer().writeAll("OK\n");
}

fn handleSnapshot(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    const index = try getIndex(ctx, req, res, true) orelse return;
    defer releaseIndex(ctx, index);

    // Set response headers for tar download
    res.header("content-type", "application/x-tar");
    res.header("content-disposition", "attachment; filename=\"index_snapshot.tar\"");

    // Build snapshot using the dedicated module
    try snapshot.buildSnapshot(res.writer().any(), index, req.arena);
}

fn handleMetrics(ctx: *Context, req: *httpz.Request, res: *httpz.Response) !void {
    _ = ctx;
    _ = req;

    const writer = res.writer();

    try metrics.writeMetrics(writer);
    try httpz.writeMetrics(writer);
}
