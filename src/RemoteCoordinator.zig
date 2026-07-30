// A Coordinator that talks to the coordinator server over HTTP (dusty client):
// data append POSTs, data read long-polls GET, meta create/delete/read on the
// registry. PG-free — the coordinator (or an external AcoustID component) owns the
// actual log. Implements the same vtable as MemoryCoordinator, so the Replicator
// can't tell the difference.
//
// The data feed is keyed by (index_name, generation) — a lineage. read() returns
// entries whose hashes are borrowed until the next read (the contract); since there
// is one consumer per index name, we decode each into its own arena (reset per
// read), so concurrent reads for different indexes don't clash.

const std = @import("std");
const zio = @import("zio");
const http = @import("dusty");
const msgpack = @import("msgpack");
const changelog_mod = @import("Coordinator.zig");
const Coordinator = changelog_mod.Coordinator;
const Entry = changelog_mod.Entry;
const Change = @import("change.zig").Change;
const MetaOp = changelog_mod.MetaOp;
const AppendRequest = changelog_mod.AppendRequest;
const AppendResponse = changelog_mod.AppendResponse;
const ReadResponse = changelog_mod.ReadResponse;
const MetaReadResponse = changelog_mod.MetaReadResponse;
const MetaCreateResponse = changelog_mod.MetaCreateResponse;
const MetaDeleteResponse = changelog_mod.MetaDeleteResponse;

const Self = @This();

// Floor on the gap between polls, so a server that reports 0 (or omits the field
// entirely) cannot turn this into a busy loop. The server only sends 0 alongside a
// full batch, which returns before any sleep — but the floor is what makes that a
// property of this client rather than a promise the server has to keep.
const min_poll_ms: u64 = 50;

// Enough for any real feed URL: base_url and index name plus three u64s and ~30
// characters of fixed text. bufPrint returns error.NoSpaceLeft if a base URL or
// index name ever exceeds it, which consumeLoop logs and retries — a loud,
// repeating failure naming its own cause, rather than a truncated request.
const max_url_len = 512;

allocator: std.mem.Allocator,
io: std.Io,
base_url: []const u8, // e.g. "http://127.0.0.1:9000"
mutex: zio.Mutex = .init,
read_arenas: std.StringHashMapUnmanaged(*std.heap.ArenaAllocator) = .empty,
meta_arena: std.heap.ArenaAllocator, // single meta consumer -> one arena

pub fn init(allocator: std.mem.Allocator, io: std.Io, base_url: []const u8) Self {
    return .{ .allocator = allocator, .io = io, .base_url = base_url, .meta_arena = std.heap.ArenaAllocator.init(allocator) };
}

pub fn deinit(self: *Self) void {
    var it = self.read_arenas.iterator();
    while (it.next()) |e| {
        e.value_ptr.*.deinit();
        self.allocator.destroy(e.value_ptr.*);
        self.allocator.free(e.key_ptr.*);
    }
    self.read_arenas.deinit(self.allocator);
    self.meta_arena.deinit();
}

pub fn coordinator(self: *Self) Coordinator {
    return .{ .ptr = self, .vtable = &vtable };
}

const vtable: Coordinator.VTable = .{
    .append = appendImpl,
    .read = readImpl,
    .createIndex = createIndexImpl,
    .deleteIndex = deleteIndexImpl,
    .readMeta = readMetaImpl,
    .setRetentionFloor = setRetentionFloorImpl,
};

fn appendImpl(ptr: *anyopaque, index_name: []const u8, generation: u64, changes: []const Change, expected: ?u64) anyerror!u64 {
    const self: *Self = @ptrCast(@alignCast(ptr));
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var aw: std.Io.Writer.Allocating = .init(a);
    try msgpack.encode(AppendRequest{ .changes = @constCast(changes), .expected = expected }, &aw.writer);
    const url = try std.fmt.allocPrint(a, "{s}/_changelog/{s}/{d}", .{ self.base_url, index_name, generation });

    var client = http.Client.init(self.allocator, self.io, .{});
    defer client.deinit();
    var resp = try client.fetch(url, .{ .method = .post, .body = aw.written() });
    defer resp.deinit();
    if (resp.status() != .ok) return statusToError(resp.status());

    const body = (try resp.body()) orelse return error.EmptyResponse;
    const ares = try msgpack.decodeFromSliceLeaky(AppendResponse, a, body);
    return ares.id;
}

fn readImpl(ptr: *anyopaque, index_name: []const u8, generation: u64, after: u64, out: []Entry, deadline: zio.Timeout) anyerror!usize {
    const self: *Self = @ptrCast(@alignCast(ptr));

    const arena = try self.arenaFor(index_name);
    _ = arena.reset(.retain_capacity); // frees the previous read's entries
    const ra = arena.allocator();

    // On the stack, not in the arena: the loop below resets the arena between
    // polls, and this outlives every one of them. Keeping it out of any allocator
    // means there is no lifetime to get wrong.
    var url_buf: [max_url_len]u8 = undefined;
    const url = try std.fmt.bufPrint(&url_buf, "{s}/_changelog/{s}/{d}?after={d}&max={d}", .{
        self.base_url, index_name, generation, after, out.len,
    });

    // The server answers immediately rather than long-polling, so the blocking
    // half of the vtable contract is implemented here: poll, and sleep for as
    // long as the server asks between attempts, until there is something to
    // return or the deadline passes. Nothing above this sees retry_after_ms.
    const until = pollUntil(deadline);
    while (true) {
        var client = http.Client.init(self.allocator, self.io, .{});
        defer client.deinit();
        var resp = try client.fetch(url, .{ .method = .get });
        defer resp.deinit();
        if (resp.status() != .ok) return statusToError(resp.status());

        const body = (try resp.body()) orelse return 0;
        const rres = try msgpack.decodeFromSliceLeaky(ReadResponse, ra, body);
        const n = @min(rres.entries.len, out.len);
        if (n > 0) {
            for (rres.entries[0..n], 0..) |e, i| out[i] = e;
            return n;
        }

        // Nothing yet. Reclaim the empty response before waiting, or an
        // indefinite poll would hold one dead ReadResponse per second for as long
        // as it runs.
        const nap = napFor(rres.retry_after_ms, until) orelse return 0;
        _ = arena.reset(.retain_capacity);
        // Sleep the server's hint, clamped to what is left of the deadline so a
        // large hint cannot overshoot it.
        try zio.sleep(nap);
    }
}

/// Absolute point to stop polling at, or null for "no deadline" — `.none` means
/// block indefinitely, and only task cancellation ends that.
fn pollUntil(deadline: zio.Timeout) ?zio.Timestamp {
    return switch (deadline) {
        .none => null,
        .duration => |d| zio.Timestamp.now(.monotonic).addDuration(d),
        .deadline => |ts| ts,
    };
}

/// How long to sleep before the next poll, or null if the deadline has passed.
/// Never sleeps zero: a server hint of 0 means "there is probably more, come
/// straight back", which only arrives with a full batch, and a full batch has
/// already returned by the time this is called. Treating it as 0 here would spin.
fn napFor(retry_after_ms: u64, until: ?zio.Timestamp) ?zio.Duration {
    const wanted = zio.Duration.fromMilliseconds(@max(retry_after_ms, min_poll_ms));
    const stop = until orelse return wanted;

    const now = zio.Timestamp.now(.monotonic);
    if (now.toNanoseconds() >= stop.toNanoseconds()) return null;
    const left = now.durationTo(stop);
    return if (left.toNanoseconds() < wanted.toNanoseconds()) left else wanted;
}

fn createIndexImpl(ptr: *anyopaque, name: []const u8) anyerror!u64 {
    const self: *Self = @ptrCast(@alignCast(ptr));
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const url = try std.fmt.allocPrint(a, "{s}/_index/{s}", .{ self.base_url, name });
    var client = http.Client.init(self.allocator, self.io, .{});
    defer client.deinit();
    // PUT, not POST: the URI already names the index, so the client supplies the
    // identity, and createIndex is documented idempotent -- an existing active
    // name returns its generation without appending a duplicate op. That is PUT's
    // defining property, and it pairs with the DELETE on the same path.
    var resp = try client.fetch(url, .{ .method = .put });
    defer resp.deinit();
    if (resp.status() != .ok) return statusToError(resp.status());

    const body = (try resp.body()) orelse return error.EmptyResponse;
    const cres = try msgpack.decodeFromSliceLeaky(MetaCreateResponse, a, body);
    return cres.generation;
}

fn deleteIndexImpl(ptr: *anyopaque, name: []const u8) anyerror!u64 {
    const self: *Self = @ptrCast(@alignCast(ptr));
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const url = try std.fmt.allocPrint(a, "{s}/_index/{s}", .{ self.base_url, name });
    var client = http.Client.init(self.allocator, self.io, .{});
    defer client.deinit();
    var resp = try client.fetch(url, .{ .method = .delete });
    defer resp.deinit();
    if (resp.status() != .ok) return statusToError(resp.status());

    const body = (try resp.body()) orelse return error.EmptyResponse;
    const dres = try msgpack.decodeFromSliceLeaky(MetaDeleteResponse, a, body);
    return dres.pos;
}

fn readMetaImpl(ptr: *anyopaque, after: u64, out: []MetaOp, deadline: zio.Timeout) anyerror!usize {
    const self: *Self = @ptrCast(@alignCast(ptr));

    _ = self.meta_arena.reset(.retain_capacity);
    const ra = self.meta_arena.allocator();

    // On the stack -- see readImpl.
    var url_buf: [max_url_len]u8 = undefined;
    const url = try std.fmt.bufPrint(&url_buf, "{s}/_meta?after={d}&max={d}", .{
        self.base_url, after, out.len,
    });

    // Same poll-and-sleep as readImpl; see there for why the server does not block.
    const until = pollUntil(deadline);
    while (true) {
        var client = http.Client.init(self.allocator, self.io, .{});
        defer client.deinit();
        var resp = try client.fetch(url, .{ .method = .get });
        defer resp.deinit();
        if (resp.status() != .ok) return statusToError(resp.status());

        const body = (try resp.body()) orelse return 0;
        const mres = try msgpack.decodeFromSliceLeaky(MetaReadResponse, ra, body);
        const n = @min(mres.ops.len, out.len);
        if (n > 0) {
            for (mres.ops[0..n], 0..) |op, i| out[i] = op;
            return n;
        }

        const nap = napFor(mres.retry_after_ms, until) orelse return 0;
        _ = self.meta_arena.reset(.retain_capacity);
        try zio.sleep(nap);
    }
}

fn setRetentionFloorImpl(ptr: *anyopaque, index_name: []const u8, generation: u64, floor: u64) anyerror!void {
    const self: *Self = @ptrCast(@alignCast(ptr));
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const url = try std.fmt.allocPrint(a, "{s}/_truncate/{s}/{d}?floor={d}", .{ self.base_url, index_name, generation, floor });
    var client = http.Client.init(self.allocator, self.io, .{});
    defer client.deinit();
    var resp = try client.fetch(url, .{ .method = .post });
    defer resp.deinit();
    if (resp.status() != .ok) return statusToError(resp.status());
}

// The long-poll window to request from the server. `.none` (block indefinitely)
// maps to the max window; the consumer loops across windows. A `.duration` (e.g.
// the meta catch-up's short deadline) is passed through so the server returns
// promptly once the feed is drained.

// One decode arena per index name (one consumer per name -> no concurrent use of
// the same arena); the map itself is guarded by the mutex.
fn arenaFor(self: *Self, index_name: []const u8) !*std.heap.ArenaAllocator {
    try self.mutex.lock();
    defer self.mutex.unlock();
    const gop = try self.read_arenas.getOrPut(self.allocator, index_name);
    if (!gop.found_existing) {
        gop.key_ptr.* = try self.allocator.dupe(u8, index_name);
        const arena = try self.allocator.create(std.heap.ArenaAllocator);
        arena.* = std.heap.ArenaAllocator.init(self.allocator);
        gop.value_ptr.* = arena;
    }
    return gop.value_ptr.*;
}

fn statusToError(status: http.Status) anyerror {
    return switch (status) {
        .conflict => error.VersionMismatch,
        .not_found => error.IndexNotFound,
        .gone => error.BelowRetention, // truncated past the requested position -> bootstrap
        else => error.CoordinatorError,
    };
}

// The poll pacing is pure arithmetic with a real off-by-one in it (clamping the
// sleep to a deadline), and it only misbehaves under timing that a live test
// would not reproduce reliably. So it is tested directly.

test "napFor: no deadline sleeps the server's hint" {
    const nap = napFor(1000, null).?;
    try std.testing.expectEqual(@as(u64, 1000), nap.toMilliseconds());
}

test "napFor: a zero hint still sleeps, so an empty answer cannot spin" {
    const nap = napFor(0, null).?;
    try std.testing.expectEqual(@as(u64, min_poll_ms), nap.toMilliseconds());
}

test "napFor: clamps to what is left of the deadline" {
    // 100ms left, server asks for 1000 -> sleep the 100, not the 1000, or the
    // read would overshoot the deadline it was given by 900ms.
    const until = zio.Timestamp.now(.monotonic).addDuration(.fromMilliseconds(100));
    const nap = napFor(1000, until).?;
    try std.testing.expect(nap.toMilliseconds() <= 100);
}

test "napFor: past the deadline returns null, which reads as zero entries" {
    const until = zio.Timestamp.now(.monotonic);
    try std.testing.expect(napFor(1000, until) == null);
}

test "pollUntil: .none never expires" {
    try std.testing.expect(pollUntil(.none) == null);
}

test "pollUntil: a duration becomes an absolute point, so polls do not restart the clock" {
    const before = zio.Timestamp.now(.monotonic);
    const until = pollUntil(.{ .duration = .fromMilliseconds(500) }).?;
    try std.testing.expect(until.toNanoseconds() > before.toNanoseconds());
}

test "statusToError: 410 is what sends a stuck node to a peer snapshot" {
    try std.testing.expectEqual(error.BelowRetention, statusToError(.gone));
}
