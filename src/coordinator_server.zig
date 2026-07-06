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

const max_read_entries = 1024;

pub const Service = struct {
    coordinator: Coordinator,
};

pub const Server = http.Server(Service);

pub fn registerRoutes(server: *Server) void {
    const r = &server.router;
    r.post("/_changelog/:index", handleAppend);
    r.get("/_changelog/:index", handleRead);
    r.post("/_index/:index", handleCreateIndex);
    r.delete("/_index/:index", handleDeleteIndex);
    r.get("/_meta", handleReadMeta);
}

fn handleCreateIndex(co: *Service, req: *http.Request, res: *http.Response) !void {
    const index = req.params.get("index") orelse return fail(res, .bad_request, "missing index");
    const generation = co.coordinator.createIndex(index) catch |err|
        return fail(res, statusFor(err), @errorName(err));
    try respond(MetaCreateResponse{ .generation = generation }, res);
}

fn handleDeleteIndex(co: *Service, req: *http.Request, res: *http.Response) !void {
    const index = req.params.get("index") orelse return fail(res, .bad_request, "missing index");
    co.coordinator.deleteIndex(index) catch |err|
        return fail(res, statusFor(err), @errorName(err));
    res.status = .no_content;
}

fn handleReadMeta(co: *Service, req: *http.Request, res: *http.Response) !void {
    const after = queryInt(req, "after") orelse 0;
    const max: usize = @intCast(@min(queryInt(req, "max") orelse 256, max_read_entries));
    const timeout_ms = queryInt(req, "timeout_ms") orelse 0;

    const buf = try req.arena.alloc(MetaOp, max);
    const deadline: zio.Timeout = if (timeout_ms == 0) .none else .{ .duration = .fromMilliseconds(timeout_ms) };
    const n = co.coordinator.readMeta(after, buf, deadline) catch |err|
        return fail(res, statusFor(err), @errorName(err));
    try respond(MetaReadResponse{ .ops = buf[0..n] }, res);
}

fn handleAppend(co: *Service, req: *http.Request, res: *http.Response) !void {
    const index = req.params.get("index") orelse return fail(res, .bad_request, "missing index");
    const body = (req.body() catch null) orelse return fail(res, .bad_request, "missing body");
    const areq = msgpack.decodeFromSliceLeaky(AppendRequest, req.arena, body) catch
        return fail(res, .bad_request, "bad body");
    const id = co.coordinator.append(index, areq.changes, areq.expected) catch |err|
        return fail(res, statusFor(err), @errorName(err));
    try respond(AppendResponse{ .id = id }, res);
}

fn handleRead(co: *Service, req: *http.Request, res: *http.Response) !void {
    const index = req.params.get("index") orelse return fail(res, .bad_request, "missing index");
    const after = queryInt(req, "after") orelse 0;
    const max: usize = @intCast(@min(queryInt(req, "max") orelse 256, max_read_entries));
    const timeout_ms = queryInt(req, "timeout_ms") orelse 0;

    const buf = try req.arena.alloc(Entry, max);
    const deadline: zio.Timeout = if (timeout_ms == 0) .none else .{ .duration = .fromMilliseconds(timeout_ms) };
    const n = co.coordinator.read(index, after, buf, deadline) catch |err|
        return fail(res, statusFor(err), @errorName(err));
    try respond(ReadResponse{ .entries = buf[0..n] }, res);
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
        else => .internal_server_error,
    };
}
