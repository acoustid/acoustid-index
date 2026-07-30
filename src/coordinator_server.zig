// The changelog coordinator: serves append/read of a backing Changelog over HTTP
// (msgpack), so fpindex replicas consume the log without touching PG. dusty runs
// each connection in its own coroutine, so the read handler can long-poll — park
// in changelog.read up to the client's timeout — cheaply (a coroutine, not a
// thread). Backed by a MemoryChangelog today; a PG-backed Changelog later, with
// nothing here changing.

const std = @import("std");
const zio = @import("zio");
const http = @import("dusty");
const msgpack = @import("msgpack");
const changelog_mod = @import("Coordinator.zig");
const Coordinator = changelog_mod.Coordinator;
const Entry = changelog_mod.Entry;
const MetaOp = changelog_mod.MetaOp;
const AppendRequest = changelog_mod.AppendRequest;
const AppendResponse = changelog_mod.AppendResponse;
const ReadResponse = changelog_mod.ReadResponse;
const MetaReadResponse = changelog_mod.MetaReadResponse;
const MetaCreateResponse = changelog_mod.MetaCreateResponse;
const MetaDeleteResponse = changelog_mod.MetaDeleteResponse;
const EmptyResponse = struct {
    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

const max_read_entries = 1024;

// This server does not long-poll. It reads whatever the coordinator has right now
// and tells the client how long to wait before asking again, which is the same
// protocol the PG-backed feed in acoustid-server speaks — so the two are
// interchangeable behind RemoteCoordinator, and the client path under test here is
// the one used against the real feed.
const no_wait: zio.Timeout = .{ .duration = .fromMilliseconds(0) };
const idle_retry_ms: u64 = 1000;
// A full batch: more is probably queued, so coming straight back costs a round
// trip and saves a wait.
const busy_retry_ms: u64 = 0;

pub const Service = struct {
    coordinator: Coordinator,
};

pub const Server = http.Server(Service);

pub fn registerRoutes(server: *Server) void {
    const r = &server.router;
    r.post("/_changelog/:index/:gen", handleAppend);
    r.get("/_changelog/:index/:gen", handleRead);
    // PUT rather than POST: the path names the index, and createIndex is
    // idempotent (an active name returns its existing generation without
    // appending a duplicate op), which is exactly what PUT means. It also matches
    // the DELETE below instead of sitting inconsistently next to it.
    r.put("/_index/:index", handleCreateIndex);
    r.delete("/_index/:index", handleDeleteIndex);
    r.get("/_meta", handleReadMeta);
    r.post("/_truncate/:index/:gen", handleTruncate);
}

fn handleTruncate(co: *Service, req: *http.Request, res: *http.Response) !void {
    const index = req.params.get("index") orelse return fail(res, .bad_request, "missing index");
    const generation = genParam(req) orelse return fail(res, .bad_request, "bad generation");
    const floor = queryInt(req, "floor") orelse return fail(res, .bad_request, "missing floor");
    co.coordinator.setRetentionFloor(index, generation, floor) catch |err|
        return fail(res, statusFor(err), @errorName(err));
    try respond(EmptyResponse{}, res);
}

fn handleCreateIndex(co: *Service, req: *http.Request, res: *http.Response) !void {
    const index = req.params.get("index") orelse return fail(res, .bad_request, "missing index");
    const generation = co.coordinator.createIndex(index) catch |err|
        return fail(res, statusFor(err), @errorName(err));
    try respond(MetaCreateResponse{ .generation = generation }, res);
}

fn handleDeleteIndex(co: *Service, req: *http.Request, res: *http.Response) !void {
    const index = req.params.get("index") orelse return fail(res, .bad_request, "missing index");
    const pos = co.coordinator.deleteIndex(index) catch |err|
        return fail(res, statusFor(err), @errorName(err));
    try respond(MetaDeleteResponse{ .pos = pos }, res);
}

fn handleReadMeta(co: *Service, req: *http.Request, res: *http.Response) !void {
    const after = queryInt(req, "after") orelse 0;
    const max: usize = @intCast(@min(queryInt(req, "max") orelse 256, max_read_entries));
    const buf = try req.arena.alloc(MetaOp, max);
    // Answer with what is there now; the client paces itself on retry_after_ms.
    // See Coordinator.ReadResponse.retry_after_ms for why nothing blocks here.
    const n = co.coordinator.readMeta(after, buf, no_wait) catch |err|
        return fail(res, statusFor(err), @errorName(err));
    try respond(MetaReadResponse{
        .ops = buf[0..n],
        .retry_after_ms = if (n == buf.len) busy_retry_ms else idle_retry_ms,
    }, res);
}

fn handleAppend(co: *Service, req: *http.Request, res: *http.Response) !void {
    const index = req.params.get("index") orelse return fail(res, .bad_request, "missing index");
    const generation = genParam(req) orelse return fail(res, .bad_request, "bad generation");
    const body = (req.body() catch null) orelse return fail(res, .bad_request, "missing body");
    const areq = msgpack.decodeFromSliceLeaky(AppendRequest, req.arena, body) catch
        return fail(res, .bad_request, "bad body");
    const id = co.coordinator.append(index, generation, areq.changes, areq.expected) catch |err|
        return fail(res, statusFor(err), @errorName(err));
    try respond(AppendResponse{ .id = id }, res);
}

fn handleRead(co: *Service, req: *http.Request, res: *http.Response) !void {
    const index = req.params.get("index") orelse return fail(res, .bad_request, "missing index");
    const generation = genParam(req) orelse return fail(res, .bad_request, "bad generation");
    const after = queryInt(req, "after") orelse 0;
    const max: usize = @intCast(@min(queryInt(req, "max") orelse 256, max_read_entries));
    const buf = try req.arena.alloc(Entry, max);
    const n = co.coordinator.read(index, generation, after, buf, no_wait) catch |err|
        return fail(res, statusFor(err), @errorName(err));
    try respond(ReadResponse{
        .entries = buf[0..n],
        // A full batch means there is probably more behind it, so do not make the
        // consumer wait to find out; anything less means it is caught up.
        .retry_after_ms = if (n == buf.len) busy_retry_ms else idle_retry_ms,
    }, res);
}

fn genParam(req: *http.Request) ?u64 {
    const v = req.params.get("gen") orelse return null;
    return std.fmt.parseInt(u64, v, 10) catch null;
}

fn queryInt(req: *http.Request, name: []const u8) ?u64 {
    const v = req.query.get(name) orelse return null;
    return std.fmt.parseInt(u64, v, 10) catch null;
}

fn respond(value: anytype, res: *http.Response) !void {
    try msgpack.encode(value, res.writer());
    try res.header("Content-Type", comptime http.ContentType.msgpack.toContentType());
}

fn fail(res: *http.Response, status: http.Status, msg: []const u8) void {
    res.status = status;
    res.body = msg;
}

fn statusFor(err: anyerror) http.Status {
    return switch (err) {
        error.VersionMismatch => .conflict,
        error.IndexNotFound => .not_found,
        error.BelowRetention => .gone, // the requested position was truncated
        else => .internal_server_error,
    };
}
