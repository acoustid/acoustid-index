const std = @import("std");
const http = @import("dusty");
const msgpack = @import("msgpack");
const api = @import("api.zig");
const Change = @import("change.zig").Change;
const MultiIndex = @import("MultiIndex.zig");
const snapshot = @import("snapshot.zig");

pub const Server = http.Server(MultiIndex);

pub fn registerRoutes(server: *Server) void {
    const r = &server.router;

    r.get("/_metrics", handleMetrics);
    r.get("/_health", handleHealth);
    r.head("/_health", handleHealth);

    r.get("/:index/_health", handleIndexHealth);
    r.head("/:index/_health", handleIndexHealth);

    r.post("/:index/_search", handleSearch);
    r.post("/:index/_update", handleUpdate);

    r.head("/:index/:id", handleHeadFingerprint);
    r.get("/:index/:id", handleGetFingerprint);
    r.put("/:index/:id", handlePutFingerprint);
    r.delete("/:index/:id", handleDeleteFingerprint);

    r.head("/:index", handleHeadIndex);
    r.get("/:index", handleGetIndex);
    r.put("/:index", handlePutIndex);
    r.delete("/:index", handleDeleteIndex);

    r.get("/:index/_snapshot", handleSnapshotExport);
}

// --- helpers ---

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
        error.BadRequest, error.InvalidIndexName, error.GenerationNotAllowed, error.InvalidFingerprintId => .bad_request,
        error.IndexNotFound, error.FingerprintNotFound => .not_found,
        error.IndexNotReady, error.SearchTimeout, error.ReplicationTimeout, error.CoordinatorError => .service_unavailable,
        error.VersionMismatch, error.IndexAlreadyExists, error.OlderIndexAlreadyExists, error.NewerIndexAlreadyExists => .conflict,
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

fn handleMetrics(mi: *MultiIndex, _: *http.Request, res: *http.Response) !void {
    try mi.writeMetrics(res.writer());
    try res.header("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
}

fn handleHealth(_: *MultiIndex, _: *http.Request, res: *http.Response) !void {
    res.body = "OK\n";
}

fn handleIndexHealth(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const exists = mi.checkIndexExists(indexName(req)) catch |err| return sendError(req, res, err);
    if (exists) {
        res.body = "OK\n";
    } else {
        res.status = .not_found;
    }
}

// --- search / update ---

fn handleSearch(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    var request = requireBody(api.SearchRequest, req, res) orelse return;
    // Sanitize untrusted request values (the legacy front-end passes trusted ones).
    request.limit = @max(@min(request.limit, api.max_search_limit), api.min_search_limit);
    request.timeout = @min(request.timeout, api.max_search_timeout);
    const response = mi.search(req.arena, indexName(req), request) catch |err| return sendError(req, res, err);
    try respond(response, req, res);
}

fn handleUpdate(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const request = requireBody(api.UpdateRequest, req, res) orelse return;
    const response = mi.update(req.arena, indexName(req), request) catch |err| return sendError(req, res, err);
    try respond(response, req, res);
}

// --- single fingerprint (sugar over _update) ---

const PutFingerprintRequest = struct {
    hashes: []u32,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

fn handleHeadFingerprint(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const id = fingerprintId(req) catch |err| return sendError(req, res, err);
    const exists = mi.checkFingerprintExists(indexName(req), id) catch |err| return sendError(req, res, err);
    if (!exists) res.status = .not_found;
}

fn handleGetFingerprint(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const id = fingerprintId(req) catch |err| return sendError(req, res, err);
    const response = mi.getFingerprintInfo(req.arena, indexName(req), id) catch |err| return sendError(req, res, err);
    try respond(response, req, res);
}

fn handlePutFingerprint(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const id = fingerprintId(req) catch |err| return sendError(req, res, err);
    const body = requireBody(PutFingerprintRequest, req, res) orelse return;
    const request = api.UpdateRequest{
        .changes = &[_]Change{.{ .insert = .{ .id = id, .hashes = body.hashes } }},
    };
    _ = mi.update(req.arena, indexName(req), request) catch |err| return sendError(req, res, err);
    try sendEmpty(req, res);
}

fn handleDeleteFingerprint(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const id = fingerprintId(req) catch |err| return sendError(req, res, err);
    const request = api.UpdateRequest{
        .changes = &[_]Change{.{ .delete = .{ .id = id } }},
    };
    _ = mi.update(req.arena, indexName(req), request) catch |err| return sendError(req, res, err);
    try sendEmpty(req, res);
}

// --- index management ---

fn handleHeadIndex(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const exists = mi.checkIndexExists(indexName(req)) catch |err| return sendError(req, res, err);
    if (!exists) res.status = .not_found;
}

fn handleGetIndex(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const response = mi.getIndexInfo(req.arena, indexName(req)) catch |err| return sendError(req, res, err);
    try respond(response, req, res);
}

fn handlePutIndex(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const request = optionalBody(api.CreateIndexRequest, req, res, .{}) orelse return;
    const response = mi.createIndex(indexName(req), request) catch |err| return sendError(req, res, err);
    if (!response.ready) res.status = .accepted; // 202 until ready
    try respond(response, req, res);
}

fn handleDeleteIndex(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const request = optionalBody(api.DeleteIndexRequest, req, res, .{}) orelse return;
    const response = mi.deleteIndex(indexName(req), request) catch |err| return sendError(req, res, err);
    try respond(response, req, res);
}

// Adapts snapshot.writeSnapshot's *std.Io.Writer onto dusty's chunked response. Zero
// internal buffer, so each write (notably the large resident segment slices) is handed
// straight to res.chunk -> the socket, never copied through an intermediate buffer.
var chunk_no_buf: [0]u8 = .{};

const ChunkedWriter = struct {
    res: *http.Response,
    interface: std.Io.Writer,

    fn init(res: *http.Response) ChunkedWriter {
        return .{ .res = res, .interface = .{ .buffer = &chunk_no_buf, .vtable = &.{ .drain = drain } } };
    }

    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *ChunkedWriter = @fieldParentPtr("interface", w);
        var total: usize = 0;
        for (data[0 .. data.len - 1]) |seg| {
            self.res.chunk(seg) catch return error.WriteFailed;
            total += seg.len;
        }
        const last = data[data.len - 1];
        for (0..splat) |_| {
            self.res.chunk(last) catch return error.WriteFailed;
            total += last.len;
        }
        return w.consume(total);
    }
};

fn handleSnapshotExport(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    // Acquire the pinned snapshot first: this is the only step that can fail before any
    // bytes go out, so error reporting still works. Once chunking starts, a mid-stream
    // failure just breaks the connection (the restorer retries another donor).
    var src = mi.acquireSnapshot(indexName(req)) catch |err| return sendError(req, res, err);
    defer src.reader.deinit();

    res.header("Content-Type", "application/octet-stream") catch {};
    var cw = ChunkedWriter.init(res);
    try snapshot.writeSnapshot(&cw.interface, req.arena, src.reader.snapshot.value, src.generation);
    try cw.interface.flush();
}
