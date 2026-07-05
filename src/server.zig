const std = @import("std");
const http = @import("dusty");
const api = @import("api.zig");
const Change = @import("change.zig").Change;
const MultiIndex = @import("MultiIndex.zig");

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

fn sendError(res: *http.Response, err: anyerror) void {
    res.status = switch (err) {
        error.BadRequest, error.InvalidIndexName => .bad_request,
        error.IndexNotFound, error.FingerprintNotFound => .not_found,
        error.IndexNotReady => .service_unavailable,
        error.VersionMismatch, error.IndexAlreadyExists => .conflict,
        error.NotImplemented => .not_implemented,
        else => .internal_server_error,
    };
    res.json(.{ .@"error" = @errorName(err) }, .{}) catch {
        res.body = "{\"error\":\"internal\"}";
    };
}

fn sendEmpty(res: *http.Response) !void {
    try res.json(.{}, .{});
}

/// Parse a required JSON body. On a missing or malformed body it responds 400
/// and returns null so the caller can `orelse return`.
fn requireJson(comptime T: type, req: *http.Request, res: *http.Response) ?T {
    const maybe = req.json(T) catch {
        sendError(res, error.BadRequest);
        return null;
    };
    return maybe orelse {
        sendError(res, error.BadRequest);
        return null;
    };
}

/// Parse an optional JSON body, falling back to a default when absent. A
/// malformed body still responds 400 and returns null.
fn optionalJson(comptime T: type, req: *http.Request, res: *http.Response, default: T) ?T {
    return (req.json(T) catch {
        sendError(res, error.BadRequest);
        return null;
    }) orelse default;
}

// --- system ---

fn handleMetrics(_: *MultiIndex, _: *http.Request, res: *http.Response) !void {
    // TODO: Prometheus metrics.
    res.body = "";
}

fn handleHealth(_: *MultiIndex, _: *http.Request, res: *http.Response) !void {
    res.body = "OK\n";
}

fn handleIndexHealth(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const exists = mi.checkIndexExists(indexName(req)) catch |err| return sendError(res, err);
    if (exists) {
        res.body = "OK\n";
    } else {
        res.status = .not_found;
    }
}

// --- search / update ---

fn handleSearch(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const request = requireJson(api.SearchRequest, req, res) orelse return;
    const response = mi.search(req.arena, indexName(req), request) catch |err| return sendError(res, err);
    try res.json(response, .{});
}

fn handleUpdate(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const request = requireJson(api.UpdateRequest, req, res) orelse return;
    const response = mi.update(req.arena, indexName(req), request) catch |err| return sendError(res, err);
    try res.json(response, .{});
}

// --- single fingerprint (sugar over _update) ---

const PutFingerprintRequest = struct {
    hashes: []u32,
};

fn handleHeadFingerprint(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const id = fingerprintId(req) catch |err| return sendError(res, err);
    const exists = mi.checkFingerprintExists(indexName(req), id) catch |err| return sendError(res, err);
    if (!exists) res.status = .not_found;
}

fn handleGetFingerprint(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const id = fingerprintId(req) catch |err| return sendError(res, err);
    const response = mi.getFingerprintInfo(req.arena, indexName(req), id) catch |err| return sendError(res, err);
    try res.json(response, .{});
}

fn handlePutFingerprint(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const id = fingerprintId(req) catch |err| return sendError(res, err);
    const body = requireJson(PutFingerprintRequest, req, res) orelse return;
    const request = api.UpdateRequest{
        .changes = &[_]Change{.{ .insert = .{ .id = id, .hashes = body.hashes } }},
    };
    _ = mi.update(req.arena, indexName(req), request) catch |err| return sendError(res, err);
    try sendEmpty(res);
}

fn handleDeleteFingerprint(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const id = fingerprintId(req) catch |err| return sendError(res, err);
    const request = api.UpdateRequest{
        .changes = &[_]Change{.{ .delete = .{ .id = id } }},
    };
    _ = mi.update(req.arena, indexName(req), request) catch |err| return sendError(res, err);
    try sendEmpty(res);
}

// --- index management ---

fn handleHeadIndex(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const exists = mi.checkIndexExists(indexName(req)) catch |err| return sendError(res, err);
    if (!exists) res.status = .not_found;
}

fn handleGetIndex(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const response = mi.getIndexInfo(req.arena, indexName(req)) catch |err| return sendError(res, err);
    try res.json(response, .{});
}

fn handlePutIndex(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const request = optionalJson(api.CreateIndexRequest, req, res, .{}) orelse return;
    _ = mi.createIndex(indexName(req), request) catch |err| return sendError(res, err);
    try sendEmpty(res);
}

fn handleDeleteIndex(mi: *MultiIndex, req: *http.Request, res: *http.Response) !void {
    const request = optionalJson(api.DeleteIndexRequest, req, res, .{}) orelse return;
    _ = mi.deleteIndex(indexName(req), request) catch |err| return sendError(res, err);
    try sendEmpty(res);
}

fn handleSnapshotExport(_: *MultiIndex, _: *http.Request, res: *http.Response) !void {
    // TODO: snapshot export (bootstrap path).
    sendError(res, error.NotImplemented);
}
