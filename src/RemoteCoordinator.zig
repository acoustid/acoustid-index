// A Changelog that talks to a coordinator over HTTP (dusty client): append POSTs,
// read long-polls GET. PG-free — the coordinator (or an external AcoustID
// component) owns the actual log. Implements the same vtable as MemoryChangelog,
// so the Replicator can't tell the difference.
//
// read() returns entries whose hashes are borrowed until the next read (the
// Changelog contract). Since there is one consumer per index, we decode each
// index's response into its own arena (reset per read), so concurrent reads for
// different indexes don't clash.

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

const Self = @This();
const long_poll_ms = 20_000;

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
};

fn appendImpl(ptr: *anyopaque, index_name: []const u8, changes: []const Change, expected: ?u64) anyerror!u64 {
    const self: *Self = @ptrCast(@alignCast(ptr));
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var aw: std.Io.Writer.Allocating = .init(a);
    try msgpack.encode(AppendRequest{ .changes = @constCast(changes), .expected = expected }, &aw.writer);
    const url = try std.fmt.allocPrint(a, "{s}/_changelog/{s}", .{ self.base_url, index_name });

    var client = http.Client.init(self.allocator, self.io, .{});
    defer client.deinit();
    var resp = try client.fetch(url, .{ .method = .post, .body = aw.written() });
    defer resp.deinit();
    if (resp.status() != .ok) return statusToError(resp.status());

    const body = (try resp.body()) orelse return error.EmptyResponse;
    const ares = try msgpack.decodeFromSliceLeaky(AppendResponse, a, body);
    return ares.id;
}

fn readImpl(ptr: *anyopaque, index_name: []const u8, after: u64, out: []Entry, deadline: zio.Timeout) anyerror!usize {
    const self: *Self = @ptrCast(@alignCast(ptr));
    _ = deadline; // consumer loops with .none; each call is one long-poll window

    const arena = try self.arenaFor(index_name);
    _ = arena.reset(.retain_capacity); // frees the previous read's entries
    const ra = arena.allocator();

    const url = try std.fmt.allocPrint(ra, "{s}/_changelog/{s}?after={d}&max={d}&timeout_ms={d}", .{
        self.base_url, index_name, after, out.len, long_poll_ms,
    });
    var client = http.Client.init(self.allocator, self.io, .{});
    defer client.deinit();
    var resp = try client.fetch(url, .{ .method = .get });
    defer resp.deinit();
    if (resp.status() != .ok) return statusToError(resp.status());

    const body = (try resp.body()) orelse return 0;
    const rres = try msgpack.decodeFromSliceLeaky(ReadResponse, ra, body);
    const n = @min(rres.entries.len, out.len);
    for (rres.entries[0..n], 0..) |e, i| out[i] = e;
    return n;
}

fn createIndexImpl(ptr: *anyopaque, name: []const u8) anyerror!u64 {
    const self: *Self = @ptrCast(@alignCast(ptr));
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const url = try std.fmt.allocPrint(a, "{s}/_index/{s}", .{ self.base_url, name });
    var client = http.Client.init(self.allocator, self.io, .{});
    defer client.deinit();
    var resp = try client.fetch(url, .{ .method = .post });
    defer resp.deinit();
    if (resp.status() != .ok) return statusToError(resp.status());

    const body = (try resp.body()) orelse return error.EmptyResponse;
    const cres = try msgpack.decodeFromSliceLeaky(MetaCreateResponse, a, body);
    return cres.generation;
}

fn deleteIndexImpl(ptr: *anyopaque, name: []const u8) anyerror!void {
    const self: *Self = @ptrCast(@alignCast(ptr));
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();

    const url = try std.fmt.allocPrint(arena.allocator(), "{s}/_index/{s}", .{ self.base_url, name });
    var client = http.Client.init(self.allocator, self.io, .{});
    defer client.deinit();
    var resp = try client.fetch(url, .{ .method = .delete });
    defer resp.deinit();
    const s = resp.status();
    if (s != .no_content and s != .ok) return statusToError(s);
}

fn readMetaImpl(ptr: *anyopaque, after: u64, out: []MetaOp, deadline: zio.Timeout) anyerror!usize {
    const self: *Self = @ptrCast(@alignCast(ptr));
    _ = deadline; // consumer loops; each call is one long-poll window

    _ = self.meta_arena.reset(.retain_capacity);
    const ra = self.meta_arena.allocator();

    const url = try std.fmt.allocPrint(ra, "{s}/_meta?after={d}&max={d}&timeout_ms={d}", .{
        self.base_url, after, out.len, long_poll_ms,
    });
    var client = http.Client.init(self.allocator, self.io, .{});
    defer client.deinit();
    var resp = try client.fetch(url, .{ .method = .get });
    defer resp.deinit();
    if (resp.status() != .ok) return statusToError(resp.status());

    const body = (try resp.body()) orelse return 0;
    const mres = try msgpack.decodeFromSliceLeaky(MetaReadResponse, ra, body);
    const n = @min(mres.ops.len, out.len);
    for (mres.ops[0..n], 0..) |op, i| out[i] = op;
    return n;
}

// One decode arena per index (one consumer per index -> no concurrent use of the
// same arena); the map itself is guarded by the mutex.
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
        else => error.CoordinatorError,
    };
}
