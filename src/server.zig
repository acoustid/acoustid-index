const std = @import("std");
const http = @import("dusty");
const msgpack = @import("msgpack");
const api = @import("api.zig");
const Change = @import("change.zig").Change;

/// The HTTP server, generic over the index manager `M` (MultiIndex for
/// standalone, ReplicatedMultiIndex for replicated mode). `M` must expose the
/// methods the handlers call (search/update/getIndexInfo/checkIndexExists/…).
pub fn Server(comptime M: type) type {
    return http.Server(M);
}

// Handlers are free functions generic over the manager `M`; this adapts one to
// dusty's fixed handler signature for a concrete `M`.
fn HandlerWrapper(comptime M: type, comptime handler_fn: anytype) type {
    return struct {
        fn wrapper(mi: *M, req: *http.Request, res: *http.Response) !void {
            return handler_fn(M, mi, req, res);
        }
    };
}

pub fn registerRoutes(comptime M: type, server: *Server(M)) void {
    const r = &server.router;

    r.get("/_metrics", HandlerWrapper(M, handleMetrics).wrapper);
    r.get("/_health", HandlerWrapper(M, handleHealth).wrapper);
    r.head("/_health", HandlerWrapper(M, handleHealth).wrapper);

    r.get("/:index/_health", HandlerWrapper(M, handleIndexHealth).wrapper);
    r.head("/:index/_health", HandlerWrapper(M, handleIndexHealth).wrapper);

    r.post("/:index/_search", HandlerWrapper(M, handleSearch).wrapper);
    r.post("/:index/_update", HandlerWrapper(M, handleUpdate).wrapper);

    r.head("/:index/:id", HandlerWrapper(M, handleHeadFingerprint).wrapper);
    r.get("/:index/:id", HandlerWrapper(M, handleGetFingerprint).wrapper);
    r.put("/:index/:id", HandlerWrapper(M, handlePutFingerprint).wrapper);
    r.delete("/:index/:id", HandlerWrapper(M, handleDeleteFingerprint).wrapper);

    r.head("/:index", HandlerWrapper(M, handleHeadIndex).wrapper);
    r.get("/:index", HandlerWrapper(M, handleGetIndex).wrapper);
    r.put("/:index", HandlerWrapper(M, handlePutIndex).wrapper);
    r.delete("/:index", HandlerWrapper(M, handleDeleteIndex).wrapper);

    r.get("/:index/_snapshot", HandlerWrapper(M, handleSnapshotExport).wrapper);
}

// --- helpers (manager-independent) ---

fn indexName(req: *http.Request) []const u8 {
    return req.params.get("index") orelse "";
}

fn fingerprintId(req: *http.Request) !u32 {
    const raw = req.params.get("id") orelse return error.BadRequest;
    return std.fmt.parseInt(u32, raw, 10) catch error.BadRequest;
}

// --- content negotiation (JSON or MessagePack) ---

const ErrorResponse = struct {
    @"error": []const u8,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

// A struct (not `.{}`, which is a tuple -> JSON `[]`) so the empty body is an
// object `{}` / empty msgpack map.
const EmptyResponse = struct {
    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

// Format to decode the request body as: an explicit Content-Type wins; an
// explicit but unsupported type is rejected; with no header we default to
// msgpack when there's a body and JSON otherwise (matches the old server).
fn requestType(req: *http.Request) !http.ContentType {
    if (req.content_type) |ct| return switch (ct) {
        .json, .msgpack => ct,
        else => error.UnsupportedMediaType,
    };
    const has_body = (req.body() catch null) != null;
    return if (has_body) .msgpack else .json;
}

// Format to encode the response as: an explicit Accept wins, else mirror the
// request type.
fn responseType(req: *http.Request) http.ContentType {
    if (req.headers.get("Accept")) |accept| {
        const t = http.ContentType.fromContentType(accept);
        if (t == .json or t == .msgpack) return t;
    }
    return requestType(req) catch .json;
}

fn decodeAs(comptime T: type, ct: http.ContentType, bytes: []const u8, arena: std.mem.Allocator) !T {
    return switch (ct) {
        .json => std.json.parseFromSliceLeaky(T, arena, bytes, .{}),
        .msgpack => msgpack.decodeFromSliceLeaky(T, arena, bytes),
        else => unreachable,
    };
}

fn sendError(req: *http.Request, res: *http.Response, err: anyerror) void {
    res.status = switch (err) {
        error.BadRequest, error.InvalidIndexName => .bad_request,
        error.IndexNotFound, error.FingerprintNotFound => .not_found,
        error.IndexNotReady, error.SearchTimeout => .service_unavailable,
        error.VersionMismatch, error.IndexAlreadyExists => .conflict,
        error.UnsupportedMediaType => .unsupported_media_type,
        error.NotImplemented => .not_implemented,
        else => .internal_server_error,
    };
    respond(ErrorResponse{ .@"error" = @errorName(err) }, req, res) catch {
        res.body = "{\"error\":\"internal\"}";
    };
}

fn sendEmpty(req: *http.Request, res: *http.Response) !void {
    try respond(EmptyResponse{}, req, res);
}

/// Serialize `value` using the negotiated response format.
fn respond(value: anytype, req: *http.Request, res: *http.Response) !void {
    switch (responseType(req)) {
        .json => try res.json(value, .{}),
        .msgpack => {
            try msgpack.encode(value, res.writer());
            try res.header("Content-Type", comptime http.ContentType.msgpack.toContentType());
        },
        else => unreachable,
    }
}

/// Decode a required body. On a missing/malformed body or unsupported type it
/// responds and returns null so the caller can `orelse return`.
fn requireBody(comptime T: type, req: *http.Request, res: *http.Response) ?T {
    const ct = requestType(req) catch |err| {
        sendError(req, res, err);
        return null;
    };
    const bytes = (req.body() catch {
        sendError(req, res, error.BadRequest);
        return null;
    }) orelse {
        sendError(req, res, error.BadRequest);
        return null;
    };
    return decodeAs(T, ct, bytes, req.arena) catch {
        sendError(req, res, error.BadRequest);
        return null;
    };
}

/// Decode an optional body, falling back to a default when absent.
fn optionalBody(comptime T: type, req: *http.Request, res: *http.Response, default: T) ?T {
    const bytes = (req.body() catch {
        sendError(req, res, error.BadRequest);
        return null;
    }) orelse return default;
    const ct = requestType(req) catch |err| {
        sendError(req, res, err);
        return null;
    };
    return decodeAs(T, ct, bytes, req.arena) catch {
        sendError(req, res, error.BadRequest);
        return null;
    };
}

// --- system ---

fn handleMetrics(comptime M: type, mi: *M, _: *http.Request, res: *http.Response) !void {
    try mi.writeMetrics(res.writer());
    try res.header("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
}

fn handleHealth(comptime M: type, _: *M, _: *http.Request, res: *http.Response) !void {
    res.body = "OK\n";
}

fn handleIndexHealth(comptime M: type, mi: *M, req: *http.Request, res: *http.Response) !void {
    const exists = mi.checkIndexExists(indexName(req)) catch |err| return sendError(req, res, err);
    if (exists) {
        res.body = "OK\n";
    } else {
        res.status = .not_found;
    }
}

// --- search / update ---

fn handleSearch(comptime M: type, mi: *M, req: *http.Request, res: *http.Response) !void {
    var request = requireBody(api.SearchRequest, req, res) orelse return;
    // Sanitize untrusted request values (the legacy front-end passes trusted ones).
    request.limit = @max(@min(request.limit, api.max_search_limit), api.min_search_limit);
    request.timeout = @min(request.timeout, api.max_search_timeout);
    const response = mi.search(req.arena, indexName(req), request) catch |err| return sendError(req, res, err);
    try respond(response, req, res);
}

fn handleUpdate(comptime M: type, mi: *M, req: *http.Request, res: *http.Response) !void {
    const request = requireBody(api.UpdateRequest, req, res) orelse return;
    const response = mi.update(req.arena, indexName(req), request) catch |err| return sendError(req, res, err);
    try respond(response, req, res);
}

// --- single fingerprint (sugar over _update) ---

const PutFingerprintRequest = struct {
    hashes: []u32,
};

fn handleHeadFingerprint(comptime M: type, mi: *M, req: *http.Request, res: *http.Response) !void {
    const id = fingerprintId(req) catch |err| return sendError(req, res, err);
    const exists = mi.checkFingerprintExists(indexName(req), id) catch |err| return sendError(req, res, err);
    if (!exists) res.status = .not_found;
}

fn handleGetFingerprint(comptime M: type, mi: *M, req: *http.Request, res: *http.Response) !void {
    const id = fingerprintId(req) catch |err| return sendError(req, res, err);
    const response = mi.getFingerprintInfo(req.arena, indexName(req), id) catch |err| return sendError(req, res, err);
    try respond(response, req, res);
}

fn handlePutFingerprint(comptime M: type, mi: *M, req: *http.Request, res: *http.Response) !void {
    const id = fingerprintId(req) catch |err| return sendError(req, res, err);
    const body = requireBody(PutFingerprintRequest, req, res) orelse return;
    const request = api.UpdateRequest{
        .changes = &[_]Change{.{ .insert = .{ .id = id, .hashes = body.hashes } }},
    };
    _ = mi.update(req.arena, indexName(req), request) catch |err| return sendError(req, res, err);
    try sendEmpty(req, res);
}

fn handleDeleteFingerprint(comptime M: type, mi: *M, req: *http.Request, res: *http.Response) !void {
    const id = fingerprintId(req) catch |err| return sendError(req, res, err);
    const request = api.UpdateRequest{
        .changes = &[_]Change{.{ .delete = .{ .id = id } }},
    };
    _ = mi.update(req.arena, indexName(req), request) catch |err| return sendError(req, res, err);
    try sendEmpty(req, res);
}

// --- index management ---

fn handleHeadIndex(comptime M: type, mi: *M, req: *http.Request, res: *http.Response) !void {
    const exists = mi.checkIndexExists(indexName(req)) catch |err| return sendError(req, res, err);
    if (!exists) res.status = .not_found;
}

fn handleGetIndex(comptime M: type, mi: *M, req: *http.Request, res: *http.Response) !void {
    const response = mi.getIndexInfo(req.arena, indexName(req)) catch |err| return sendError(req, res, err);
    try respond(response, req, res);
}

fn handlePutIndex(comptime M: type, mi: *M, req: *http.Request, res: *http.Response) !void {
    const request = optionalBody(api.CreateIndexRequest, req, res, .{}) orelse return;
    const response = mi.createIndex(indexName(req), request) catch |err| return sendError(req, res, err);
    if (!response.ready) res.status = .accepted; // 202 until ready
    try respond(response, req, res);
}

fn handleDeleteIndex(comptime M: type, mi: *M, req: *http.Request, res: *http.Response) !void {
    const request = optionalBody(api.DeleteIndexRequest, req, res, .{}) orelse return;
    const response = mi.deleteIndex(indexName(req), request) catch |err| return sendError(req, res, err);
    try respond(response, req, res);
}

fn handleSnapshotExport(comptime M: type, _: *M, req: *http.Request, res: *http.Response) !void {
    // TODO: snapshot export (bootstrap path).
    sendError(req, res, error.NotImplemented);
}
