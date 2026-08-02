// Legacy AcoustID line protocol over TCP, served in-process (replaces the old
// Python proxy). A faithful reimplementation of the C++ server's protocol/session:
// a single "main" index, per-connection session attributes (search tuning), and a
// begin/insert/commit transaction. One line in, "OK <r>\r\n" / "ERR <msg>\r\n" out.

const std = @import("std");
const zio = @import("zio");
const MultiIndex = @import("MultiIndex.zig");
const api = @import("api.zig");
const Change = @import("change.zig").Change;
const Metadata = @import("Metadata.zig");
const log = std.log.scoped(.legacy);

const index_name = "main";
const read_buf_size = 256 * 1024; // also the max line length
const write_buf_size = 64 * 1024;

/// Accept loop. Ensures the "main" index exists, then serves connections until
/// cancelled (shutdown cancels the connection group).
pub fn listen(mi: *MultiIndex, addr: zio.net.IpAddress, read_only: bool) !void {
    _ = try mi.createIndex(index_name, .{});

    // reuse_address sets SO_REUSEADDR + SO_REUSEPORT (zio), so the port rebinds
    // promptly after a restart.
    const server = try addr.listen(.{ .reuse_address = true });
    defer server.close();
    log.info("legacy protocol listening on {f}", .{server.socket.address});

    var group: zio.Group = .init;
    defer group.cancel();

    while (true) {
        const stream = try server.accept(.{});
        errdefer stream.close();
        try group.spawn(handleConnection, .{ mi, stream, read_only });
    }
}

const Session = struct {
    alloc: std.mem.Allocator,
    read_only: bool = false, // replica: reject writes, allow searches

    // Ephemeral, per-connection tuning (C++ Session attributes).
    max_results: u32 = 500,
    top_score_percent: u32 = 10,
    timeout_ms: u32 = 0, // 0 = no timeout
    idle_timeout_ms: u32 = 60_000,

    // Transaction (open between begin and commit/rollback).
    in_txn: bool = false,
    changes: std.ArrayListUnmanaged(Change) = .empty,
    attrs: Metadata, // pending index attributes for the open transaction
    txn_arena: std.heap.ArenaAllocator, // backs buffered change hashes

    fn clearTxn(self: *Session) void {
        self.changes.clearRetainingCapacity();
        _ = self.txn_arena.reset(.retain_capacity);
        self.attrs.deinit();
        self.attrs = Metadata.initOwned(self.alloc);
    }

    fn sessionAttr(self: *Session, name: []const u8) ?*u32 {
        if (eql(name, "max_results")) return &self.max_results;
        if (eql(name, "top_score_percent")) return &self.top_score_percent;
        if (eql(name, "timeout")) return &self.timeout_ms;
        if (eql(name, "idle_timeout")) return &self.idle_timeout_ms;
        return null;
    }
};

const Response = union(enum) { ok: []const u8, err: []const u8 };

fn handleConnection(mi: *MultiIndex, stream: zio.net.Stream, read_only: bool) void {
    defer stream.close();
    defer stream.shutdown(.both) catch {};

    const alloc = mi.allocator;
    const read_buf = alloc.alloc(u8, read_buf_size) catch return;
    defer alloc.free(read_buf);
    const write_buf = alloc.alloc(u8, write_buf_size) catch return;
    defer alloc.free(write_buf);

    var reader = stream.reader(read_buf);
    var writer = stream.writer(write_buf);

    var session = Session{
        .alloc = alloc,
        .read_only = read_only,
        .attrs = Metadata.initOwned(alloc),
        .txn_arena = std.heap.ArenaAllocator.init(alloc),
    };
    defer {
        session.changes.deinit(alloc);
        session.attrs.deinit();
        session.txn_arena.deinit();
    }

    var cmd_arena = std.heap.ArenaAllocator.init(alloc);
    defer cmd_arena.deinit();

    while (true) {
        var idle: zio.AutoCancel = .init;
        idle.set(.fromMilliseconds(session.idle_timeout_ms));
        const raw = reader.interface.takeDelimiterInclusive('\n') catch |err| {
            idle.clear();
            switch (err) {
                error.EndOfStream => return, // client closed
                error.ReadFailed => {
                    if ((reader.err orelse error.ReadFailed) == error.Canceled and idle.check(error.Canceled)) {
                        reply(&writer.interface, "ERR ", "timeout") catch {};
                    }
                    return; // idle timeout, shutdown, or read failure -> drop
                },
                else => {
                    reply(&writer.interface, "ERR ", "line too long") catch {};
                    return;
                },
            }
        };
        idle.clear();
        const line = std.mem.trimEnd(u8, raw, "\r\n");

        // Preserve the C++ listener's historical behavior: any line beginning
        // with "quit" is acknowledged and then closes the connection.
        if (std.mem.startsWith(u8, line, "quit")) {
            reply(&writer.interface, "OK ", "") catch {};
            return;
        }

        _ = cmd_arena.reset(.retain_capacity);
        const resp = dispatch(mi, &session, cmd_arena.allocator(), line) catch |err| switch (err) {
            error.Canceled => return, // shutting down
            else => Response{ .err = "internal error" },
        };
        (switch (resp) {
            .ok => |p| reply(&writer.interface, "OK ", p),
            .err => |m| reply(&writer.interface, "ERR ", m),
        }) catch return;
    }
}

fn reply(w: *std.Io.Writer, prefix: []const u8, payload: []const u8) !void {
    try w.writeAll(prefix);
    try w.writeAll(payload);
    try w.writeAll("\r\n");
    try w.flush();
}

fn eql(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}

fn dispatch(mi: *MultiIndex, session: *Session, arena: std.mem.Allocator, line: []const u8) !Response {
    var it = std.mem.tokenizeScalar(u8, line, ' ');
    const cmd = it.next() orelse return .{ .ok = "" }; // empty line -> OK

    var args: std.ArrayListUnmanaged([]const u8) = .empty;
    while (it.next()) |a| try args.append(arena, a);
    const a = args.items;

    if (eql(cmd, "echo")) return .{ .ok = try std.mem.join(arena, " ", a) };
    if (eql(cmd, "search")) return search(mi, session, arena, a);
    if (eql(cmd, "insert")) return insert(session, a);
    if (eql(cmd, "begin")) {
        // Replicas are read-only over the legacy protocol; writes go via the
        // changelog. Rejecting begin blocks every transactional write.
        if (session.read_only) return .{ .err = "read-only replica" };
        if (session.in_txn) return .{ .err = "already in transaction" };
        session.clearTxn();
        session.in_txn = true;
        return .{ .ok = "" };
    }
    if (eql(cmd, "commit")) return commit(mi, session, arena);
    if (eql(cmd, "rollback")) {
        if (!session.in_txn) return .{ .err = "not in transaction" };
        session.in_txn = false;
        session.clearTxn();
        return .{ .ok = "" };
    }
    if (eql(cmd, "optimize") or eql(cmd, "cleanup")) {
        // idx-ng merges/cleans up in the background; keep the C++ transaction
        // guard, then no-op.
        if (!session.in_txn) return .{ .err = "not in transaction" };
        return .{ .ok = "" };
    }
    if (eql(cmd, "get")) return getAttribute(mi, session, arena, a);
    if (eql(cmd, "set")) return setAttribute(session, a);

    return .{ .err = "unknown command" };
}

fn search(mi: *MultiIndex, session: *Session, arena: std.mem.Allocator, args: [][]const u8) !Response {
    if (args.len != 1) return .{ .err = "expected one argument" };
    const hashes = parseFingerprint(arena, args[0]) catch |err| switch (err) {
        error.EmptyFingerprint => return .{ .err = "empty fingerprint" },
        error.InvalidFingerprint => return .{ .err = "invalid fingerprint" },
        else => return err,
    };
    const resp = mi.search(arena, index_name, .{
        .query = hashes,
        .limit = session.max_results,
        .timeout = session.timeout_ms,
        .min_score = 1,
        .score_pct = session.top_score_percent,
    }) catch |err| switch (err) {
        error.SearchTimeout => return .{ .err = "timeout exceeded" },
        error.Canceled => return err,
        else => return .{ .err = "search failed" },
    };

    var out: std.ArrayListUnmanaged(u8) = .empty;
    for (resp.results, 0..) |r, i| {
        const sep: []const u8 = if (i == 0) "" else " ";
        try out.appendSlice(arena, try std.fmt.allocPrint(arena, "{s}{d}:{d}", .{ sep, r.id, r.score }));
    }
    return .{ .ok = out.items };
}

fn insert(session: *Session, args: [][]const u8) !Response {
    if (!session.in_txn) return .{ .err = "not in transaction" };
    if (args.len != 2) return .{ .err = "expected two arguments" };
    const id = std.fmt.parseInt(u32, args[0], 10) catch return .{ .err = "invalid document id" };
    const hashes = parseFingerprint(session.txn_arena.allocator(), args[1]) catch |err| switch (err) {
        error.EmptyFingerprint => return .{ .err = "empty fingerprint" },
        error.InvalidFingerprint => return .{ .err = "invalid fingerprint" },
        else => return err,
    };
    try session.changes.append(session.alloc, .{ .insert = .{ .id = id, .hashes = hashes } });
    return .{ .ok = "" };
}

fn commit(mi: *MultiIndex, session: *Session, arena: std.mem.Allocator) !Response {
    if (!session.in_txn) return .{ .err = "not in transaction" };
    if (session.changes.items.len > 0 or session.attrs.count() > 0) {
        _ = mi.update(arena, index_name, .{
            .changes = session.changes.items,
            .metadata = if (session.attrs.count() > 0) session.attrs else null,
        }) catch |err| switch (err) {
            error.Canceled => return err,
            else => return .{ .err = "commit failed" },
        };
    }
    session.in_txn = false;
    session.clearTxn();
    return .{ .ok = "" };
}

fn getAttribute(mi: *MultiIndex, session: *Session, arena: std.mem.Allocator, args: [][]const u8) !Response {
    const name = attrName(args) orelse return .{ .err = "expected one argument" };
    if (session.sessionAttr(name)) |ptr| {
        return .{ .ok = try std.fmt.allocPrint(arena, "{d}", .{ptr.*}) };
    }
    // Index attribute -> committed metadata (empty if unset).
    const info = mi.getIndexInfo(arena, index_name) catch |err| switch (err) {
        error.Canceled => return err,
        else => return .{ .ok = "" },
    };
    return .{ .ok = info.metadata.get(name) orelse "" };
}

fn setAttribute(session: *Session, args: [][]const u8) !Response {
    var name: []const u8 = undefined;
    var value: []const u8 = undefined;
    if (args.len == 2) {
        name = args[0];
        value = args[1];
    } else if (args.len == 3 and eql(args[0], "attribute")) {
        name = args[1];
        value = args[2];
    } else {
        return .{ .err = "expected two arguments" };
    }

    if (session.sessionAttr(name)) |ptr| {
        ptr.* = std.fmt.parseInt(u32, value, 10) catch return .{ .err = "invalid value" };
        return .{ .ok = "" };
    }
    // Index attribute -> requires an open transaction (buffered until commit).
    if (!session.in_txn) return .{ .err = "not in transaction" };
    try session.attrs.set(name, value);
    return .{ .ok = "" };
}

// `get name` or (legacy) `get attribute name`.
fn attrName(args: [][]const u8) ?[]const u8 {
    if (args.len == 1) return args[0];
    if (args.len == 2 and eql(args[0], "attribute")) return args[1];
    return null;
}

// Comma-separated signed decimals, reinterpreted as u32 (clients send the signed
// form of each 32-bit hash).
fn parseFingerprint(arena: std.mem.Allocator, s: []const u8) ![]u32 {
    if (s.len == 0) return error.EmptyFingerprint;
    var list: std.ArrayListUnmanaged(u32) = .empty;
    var it = std.mem.splitScalar(u8, s, ',');
    while (it.next()) |tok| {
        const v = std.fmt.parseInt(i64, tok, 10) catch return error.InvalidFingerprint;
        try list.append(arena, @truncate(@as(u64, @bitCast(v))));
    }
    if (list.items.len == 0) return error.EmptyFingerprint;
    return list.items;
}
