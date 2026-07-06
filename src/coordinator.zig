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
const changelog_mod = @import("changelog.zig");
const Changelog = changelog_mod.Changelog;
const Entry = changelog_mod.Entry;
const AppendRequest = changelog_mod.AppendRequest;
const AppendResponse = changelog_mod.AppendResponse;
const ReadResponse = changelog_mod.ReadResponse;

const max_read_entries = 1024;

pub const Coordinator = struct {
    changelog: Changelog,
};

pub const Server = http.Server(Coordinator);

pub fn registerRoutes(server: *Server) void {
    const r = &server.router;
    r.post("/_changelog/:index", handleAppend);
    r.get("/_changelog/:index", handleRead);
}

fn handleAppend(co: *Coordinator, req: *http.Request, res: *http.Response) !void {
    const index = req.params.get("index") orelse return fail(res, .bad_request, "missing index");
    const body = (req.body() catch null) orelse return fail(res, .bad_request, "missing body");
    const areq = msgpack.decodeFromSliceLeaky(AppendRequest, req.arena, body) catch
        return fail(res, .bad_request, "bad body");
    const id = co.changelog.append(index, areq.changes, areq.expected) catch |err|
        return fail(res, statusFor(err), @errorName(err));
    try respond(AppendResponse{ .id = id }, res);
}

fn handleRead(co: *Coordinator, req: *http.Request, res: *http.Response) !void {
    const index = req.params.get("index") orelse return fail(res, .bad_request, "missing index");
    const after = queryInt(req, "after") orelse 0;
    const max: usize = @intCast(@min(queryInt(req, "max") orelse 256, max_read_entries));
    const timeout_ms = queryInt(req, "timeout_ms") orelse 0;

    const buf = try req.arena.alloc(Entry, max);
    const deadline: zio.Timeout = if (timeout_ms == 0) .none else .{ .duration = .fromMilliseconds(timeout_ms) };
    const n = co.changelog.read(index, after, buf, deadline) catch |err|
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
