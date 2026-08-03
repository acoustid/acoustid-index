const std = @import("std");
const http = @import("dusty");
const msgpack = @import("msgpack");
const api = @import("api.zig");
const Change = @import("change.zig").Change;
const MultiIndex = @import("MultiIndex.zig");
const snapshot = @import("snapshot.zig");

pub const ServerContext = struct {
    mi: *MultiIndex,

    pub fn uncaughtError(self: *ServerContext, req: *http.Request, res: *http.Response, err: anyerror) void {
        _ = self;
        sendError(req, res, err);
    }

    pub fn notFound(self: *ServerContext, req: *http.Request, res: *http.Response) !void {
        _ = self;
        sendError(req, res, error.NotFound);
    }
};

pub const Server = http.Server(ServerContext);

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
    r.get("/:index/_status", handlePeerStatus);
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
        // Not 503 — retrying will never make a read-only feed accept a write.
        error.FeedIsReadOnly => .forbidden,
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

/// Decode a required body. Missing/malformed bodies and unsupported types are
/// returned to the server context for consistent error responses.
fn requireBody(comptime T: type, req: *http.Request) !T {
    const ct = try requestType(req);
    const bytes = (try req.body()) orelse return error.BadRequest;
    return decodeAs(T, ct, bytes, req.arena) catch error.BadRequest;
}

/// Decode an optional body, falling back to a default when absent. Decode
/// failures are returned to the server context for consistent error responses.
fn optionalBody(comptime T: type, req: *http.Request, default: T) !T {
    const bytes = (try req.body()) orelse return default;
    const ct = try requestType(req);
    return decodeAs(T, ct, bytes, req.arena) catch error.BadRequest;
}

// --- system ---

fn handleMetrics(ctx: *ServerContext, _: *http.Request, res: *http.Response) !void {
    try ctx.mi.writeMetrics(res.writer());
    try res.header("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
}

fn handleHealth(_: *ServerContext, _: *http.Request, res: *http.Response) !void {
    res.body = "OK\n";
}

fn handleIndexHealth(ctx: *ServerContext, req: *http.Request, res: *http.Response) !void {
    switch (try ctx.mi.indexHealth(indexName(req))) {
        .ready => res.body = "OK\n",
        // 503 + a distinct body: the index exists but is being filled by a
        // bootstrap (initial seed or below-retention restore), and every search
        // it answered would be honest-looking but empty or stale. Non-2xx keeps a
        // search balancer's readiness probe from routing traffic here until the
        // bootstrap installs.
        .loading => {
            res.status = .service_unavailable;
            res.body = "LOADING\n";
        },
        .missing => res.status = .not_found,
    }
}

// --- search / update ---

fn handleSearch(ctx: *ServerContext, req: *http.Request, res: *http.Response) !void {
    var request = try requireBody(api.SearchRequest, req);
    // Sanitize untrusted request values (the legacy front-end passes trusted ones).
    request.limit = @max(@min(request.limit, api.max_search_limit), api.min_search_limit);
    request.timeout = @min(request.timeout, api.max_search_timeout);
    const response = try ctx.mi.search(req.arena, indexName(req), request);
    try respond(response, req, res);
}

fn handleUpdate(ctx: *ServerContext, req: *http.Request, res: *http.Response) !void {
    const request = try requireBody(api.UpdateRequest, req);
    const response = try ctx.mi.update(req.arena, indexName(req), request);
    try respond(response, req, res);
}

// --- single fingerprint (sugar over _update) ---

const PutFingerprintRequest = struct {
    hashes: []u32,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

fn handleHeadFingerprint(ctx: *ServerContext, req: *http.Request, res: *http.Response) !void {
    const id = try fingerprintId(req);
    const exists = try ctx.mi.checkFingerprintExists(indexName(req), id);
    if (!exists) res.status = .not_found;
}

fn handleGetFingerprint(ctx: *ServerContext, req: *http.Request, res: *http.Response) !void {
    const id = try fingerprintId(req);
    const response = try ctx.mi.getFingerprintInfo(req.arena, indexName(req), id);
    try respond(response, req, res);
}

fn handlePutFingerprint(ctx: *ServerContext, req: *http.Request, res: *http.Response) !void {
    const id = try fingerprintId(req);
    const body = try requireBody(PutFingerprintRequest, req);
    const request = api.UpdateRequest{
        .changes = &[_]Change{.{ .insert = .{ .id = id, .hashes = body.hashes } }},
    };
    _ = try ctx.mi.update(req.arena, indexName(req), request);
    try sendEmpty(req, res);
}

fn handleDeleteFingerprint(ctx: *ServerContext, req: *http.Request, res: *http.Response) !void {
    const id = try fingerprintId(req);
    const request = api.UpdateRequest{
        .changes = &[_]Change{.{ .delete = .{ .id = id } }},
    };
    _ = try ctx.mi.update(req.arena, indexName(req), request);
    try sendEmpty(req, res);
}

// --- index management ---

fn handleHeadIndex(ctx: *ServerContext, req: *http.Request, res: *http.Response) !void {
    const exists = try ctx.mi.checkIndexExists(indexName(req));
    if (!exists) res.status = .not_found;
}

fn handleGetIndex(ctx: *ServerContext, req: *http.Request, res: *http.Response) !void {
    const response = try ctx.mi.getIndexInfo(req.arena, indexName(req));
    try respond(response, req, res);
}

fn handlePutIndex(ctx: *ServerContext, req: *http.Request, res: *http.Response) !void {
    const request = try optionalBody(api.CreateIndexRequest, req, .{});
    const response = try ctx.mi.createIndex(indexName(req), request);
    if (!response.ready) res.status = .accepted; // 202 until ready
    try respond(response, req, res);
}

fn handleDeleteIndex(ctx: *ServerContext, req: *http.Request, res: *http.Response) !void {
    const request = try optionalBody(api.DeleteIndexRequest, req, .{});
    const response = try ctx.mi.deleteIndex(indexName(req), request);
    try respond(response, req, res);
}

const ChunkedWriter = struct {
    res: *http.Response,
    interface: std.Io.Writer,
    err: ?anyerror = null,

    fn init(res: *http.Response, buf: []u8) ChunkedWriter {
        return .{ .res = res, .interface = .{ .buffer = buf, .vtable = &.{ .drain = drain } } };
    }

    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *ChunkedWriter = @fieldParentPtr("interface", w);
        var total: usize = 0;
        for (data[0 .. data.len - 1]) |seg| {
            self.res.chunk(seg) catch |err| {
                self.err = err;
                return error.WriteFailed;
            };
            total += seg.len;
        }
        const last = data[data.len - 1];
        for (0..splat) |_| {
            self.res.chunk(last) catch |err| {
                self.err = err;
                return error.WriteFailed;
            };
            total += last.len;
        }
        return w.consume(total);
    }
};

// What this node holds for an index, so a bootstrapping peer can decide whether to
// fetch a snapshot from here. The peer-facing half of peers.findDonor; deliberately
// cheap, since every bootstrapping node probes every peer.
fn handlePeerStatus(ctx: *ServerContext, req: *http.Request, res: *http.Response) !void {
    const response = try ctx.mi.getPeerStatus(indexName(req));
    try respond(response, req, res);
}

fn handleSnapshotExport(ctx: *ServerContext, req: *http.Request, res: *http.Response) !void {
    var src = try ctx.mi.acquireSnapshot(indexName(req));
    defer src.reader.deinit();

    try res.header("Content-Type", "application/octet-stream");

    var buf: [1024]u8 = undefined;
    var cw = ChunkedWriter.init(res, &buf);
    snapshot.writeSnapshot(&cw.interface, req.arena, src.reader.snapshot.value, src.generation) catch |err| switch (err) {
        error.WriteFailed => return cw.err orelse error.WriteFailed,
        else => return err,
    };
}
