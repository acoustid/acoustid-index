// Replication component, owned by MultiIndex when running in replicated mode. It
// turns each index into a follower of an external ordered log (the Changelog):
//   - writes append to the log and wait for the local consumer to apply them
//     (read-your-writes), returning the assigned id as the version;
//   - a per-index consumer coroutine reads the log and applies each batch via
//     MultiIndex.applyLog with version = changelog id.
//
// The index (and MultiIndex.applyLog) stays oblivious to replication — this sits
// between the write API and the oblivious apply path.
//
// Locking: this component's mutex guards the consumer map + per-consumer applied
// watermark; the consumer's apply path takes the MultiIndex lock separately, and
// the two are never held at once, so there's no lock-ordering cycle.

const std = @import("std");
const zio = @import("zio");
const MultiIndex = @import("MultiIndex.zig");
const changelog_mod = @import("changelog.zig");
const Changelog = changelog_mod.Changelog;
const Entry = changelog_mod.Entry;
const Change = @import("change.zig").Change;
const api = @import("api.zig");
const log = std.log.scoped(.replicator);

const Self = @This();
const batch_size = 256;

allocator: std.mem.Allocator,
mi: *MultiIndex,
changelog: Changelog,
mutex: zio.Mutex = .init,
cond: zio.Condition = .init, // broadcast after each apply (read-your-writes)
consumers: std.StringHashMapUnmanaged(*Consumer) = .empty,

const Consumer = struct {
    name: []const u8, // owned; also the map key
    replicator: *Self,
    applied: u64, // max applied changelog id (guarded by mutex)
    task: ?zio.JoinHandle(zio.Cancelable!void) = null,
};

pub fn init(allocator: std.mem.Allocator, mi: *MultiIndex, changelog: Changelog) Self {
    return .{ .allocator = allocator, .mi = mi, .changelog = changelog };
}

pub fn deinit(self: *Self) void {
    var it = self.consumers.iterator();
    while (it.next()) |e| {
        const c = e.value_ptr.*;
        if (c.task) |*t| t.cancel(); // cancel + join
        self.allocator.free(c.name);
        self.allocator.destroy(c);
    }
    self.consumers.deinit(self.allocator);
}

/// Start a consumer for every index that already exists (called once at startup).
pub fn start(self: *Self) !void {
    const names = try self.mi.listIndexNames(self.allocator);
    defer {
        for (names) |n| self.allocator.free(n);
        self.allocator.free(names);
    }
    for (names) |name| {
        const version = try self.mi.indexVersion(name);
        try self.addConsumer(name, version);
    }
}

/// Start a consumer for `name` beginning at `start_version`. Safe to call while
/// the caller holds the MultiIndex lock: it only touches this component's lock
/// and spawns — it never calls back into MultiIndex.
pub fn addConsumer(self: *Self, name: []const u8, start_version: u64) !void {
    try self.mutex.lock();
    defer self.mutex.unlock();
    if (self.consumers.contains(name)) return;

    const name_copy = try self.allocator.dupe(u8, name);
    errdefer self.allocator.free(name_copy);
    const c = try self.allocator.create(Consumer);
    errdefer self.allocator.destroy(c);
    c.* = .{ .name = name_copy, .replicator = self, .applied = start_version };
    try self.consumers.put(self.allocator, name_copy, c);
    errdefer _ = self.consumers.remove(name_copy);
    c.task = try zio.spawn(consumeLoop, .{ c, start_version });
}

/// Stop and remove the consumer for `name`. deleteIndex calls this *before* it
/// takes the MultiIndex lock, so a consumer's in-flight apply can still finish.
pub fn removeConsumer(self: *Self, name: []const u8) void {
    self.mutex.lockUncancelable();
    const removed = self.consumers.fetchRemove(name);
    self.cond.broadcast(); // wake waiters; they'll see the consumer gone
    self.mutex.unlock();

    if (removed) |kv| {
        if (kv.value.task) |*t| t.cancel(); // cancel + join
        self.allocator.free(kv.value.name);
        self.allocator.destroy(kv.value);
    }
}

/// Write path in replicated mode: append to the log, wait for the local consumer
/// to apply up to the assigned id (read-your-writes), return that id.
pub fn update(self: *Self, name: []const u8, request: api.UpdateRequest) !api.UpdateResponse {
    {
        try self.mutex.lock();
        defer self.mutex.unlock();
        if (!self.consumers.contains(name)) return error.IndexNotFound;
    }
    const id = try self.changelog.append(name, request.changes, request.expected_version);
    try self.waitApplied(name, id);
    return .{ .version = id };
}

fn waitApplied(self: *Self, name: []const u8, id: u64) !void {
    try self.mutex.lock();
    defer self.mutex.unlock();
    while (true) {
        const c = self.consumers.get(name) orelse return error.IndexNotFound;
        if (c.applied >= id) return;
        try self.cond.wait(&self.mutex);
    }
}

fn markApplied(self: *Self, c: *Consumer, version: u64) void {
    self.mutex.lockUncancelable();
    defer self.mutex.unlock();
    c.applied = version;
    self.cond.broadcast();
}

fn consumeLoop(c: *Consumer, start_version: u64) zio.Cancelable!void {
    const self = c.replicator;
    var buf: [batch_size]Entry = undefined;
    var changes: [batch_size]Change = undefined;
    var after = start_version;
    while (true) {
        const n = self.changelog.read(c.name, after, &buf, .none) catch |err| {
            if (err == error.Canceled) return error.Canceled;
            log.warn("changelog read failed for '{s}': {}", .{ c.name, err });
            continue;
        };
        if (n == 0) continue;

        for (buf[0..n], 0..) |e, i| changes[i] = e.change;
        const version = buf[n - 1].id; // coalesce the batch; version = max id
        self.mi.applyLog(c.name, changes[0..n], null, version) catch |err| {
            if (err == error.Canceled) return error.Canceled;
            // Skip past the batch rather than spin on a persistently bad entry.
            log.warn("apply failed for '{s}' at version {d}: {}", .{ c.name, version, err });
        };
        after = version;
        self.markApplied(c, version);
    }
}

// ---- tests ----

test "replicated update flows through the changelog; RYW + search see it" {
    const MemoryChangelog = changelog_mod.MemoryChangelog;
    const common = @import("common.zig");

    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_replicator_flow";
    common.deleteDirTree(std.testing.allocator, cwd, dir_path) catch {};
    try cwd.createDir(dir_path, 0o755);
    defer common.deleteDirTree(std.testing.allocator, cwd, dir_path) catch {};
    const dir = try cwd.openDir(dir_path, .{ .iterate = true });

    // The changelog must outlive the manager (its consumers borrow it), so declare
    // it first — LIFO defers then run mi.deinit() before cl.deinit().
    var cl = MemoryChangelog.init(std.testing.allocator);
    defer cl.deinit();

    var mi = MultiIndex.init(std.testing.allocator, dir);
    defer mi.deinit();
    _ = try mi.createIndex("main", .{});
    try mi.startReplication(cl.changelog());

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Replicated write: appends to the log, blocks until the consumer applies it.
    var hashes = [_]u32{ 100, 200, 300 };
    const changes = [_]Change{.{ .insert = .{ .id = 1, .hashes = &hashes } }};
    const resp = try mi.update(a, "main", .{ .changes = &changes });
    try std.testing.expect(resp.version >= 1);

    // read-your-writes held, so search sees the doc immediately after update.
    var query = [_]u32{ 100, 200, 300 };
    const sresp = try mi.search(a, "main", .{ .query = &query });
    try std.testing.expectEqual(@as(usize, 1), sresp.results.len);
    try std.testing.expectEqual(@as(u32, 1), sresp.results[0].id);

    // A second index created *after* replication started gets its own consumer.
    _ = try mi.createIndex("other", .{});
    var h2 = [_]u32{ 7, 8, 9 };
    const c2 = [_]Change{.{ .insert = .{ .id = 42, .hashes = &h2 } }};
    _ = try mi.update(a, "other", .{ .changes = &c2 });
    var q2 = [_]u32{ 7, 8, 9 };
    const s2 = try mi.search(a, "other", .{ .query = &q2 });
    try std.testing.expectEqual(@as(usize, 1), s2.results.len);
    try std.testing.expectEqual(@as(u32, 42), s2.results[0].id);
}
