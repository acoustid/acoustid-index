// Replication component, owned by MultiIndex when running in replicated mode. It
// makes each node a follower of an external ordered log (the Coordinator):
//
//   - A single *meta consumer* coroutine follows the global meta feed (index
//     lifecycle). It reconciles local indexes against it: create -> build the
//     lineage + start its data consumer; delete -> drop it. This is what makes a
//     `create index` on one node appear on all nodes.
//   - A *data consumer* coroutine per active index follows that lineage's data feed
//     (keyed by (name, generation)) and applies each batch via MultiIndex.applyLog
//     with version = the per-lineage seq.
//   - Writes route through the coordinator and wait for THIS node's consumer to
//     apply them (read/create/delete-your-writes), returning the assigned position.
//
// The index (and MultiIndex.applyLog) stays oblivious to replication.
//
// Locking: this component's mutex guards the consumer map + per-consumer applied
// watermark + the meta_applied watermark. The consumers' apply paths take the
// MultiIndex lock separately; the two are never held at once (the meta consumer
// takes the MultiIndex lock, releases it, then takes this mutex to mark applied),
// so there is no lock-ordering cycle.

const std = @import("std");
const zio = @import("zio");
const MultiIndex = @import("MultiIndex.zig");
const coordinator_mod = @import("Coordinator.zig");
const Coordinator = coordinator_mod.Coordinator;
const Entry = coordinator_mod.Entry;
const MetaOp = coordinator_mod.MetaOp;
const Change = @import("change.zig").Change;
const api = @import("api.zig");
const log = std.log.scoped(.replicator);

const Self = @This();
const batch_size = 256;
const meta_batch = 64;
// Server-side long-poll window for the meta catch-up phase's final (empty) read.
// Only affects how long startup waits once the feed is drained — correctness does
// not depend on catch-up completeness (the streaming phase continues from `after`).
const meta_catchup_ms = 100;

allocator: std.mem.Allocator,
mi: *MultiIndex,
coordinator: Coordinator,
mutex: zio.Mutex = .init,
cond: zio.Condition = .init, // broadcast after each data apply (read-your-writes)
meta_cond: zio.Condition = .init, // broadcast after each meta apply (create/delete-your-writes)
consumers: std.StringHashMapUnmanaged(*Consumer) = .empty,
meta_applied: u64 = 0, // max meta pos reconciled (guarded by mutex)
meta_task: ?zio.JoinHandle(zio.Cancelable!void) = null,

const Consumer = struct {
    name: []const u8, // owned; also the map key
    generation: u64, // the lineage this consumer follows
    replicator: *Self,
    applied: u64, // max applied per-lineage seq (guarded by mutex)
    task: ?zio.JoinHandle(zio.Cancelable!void) = null,
};

// What survives folding the meta feed to final state per name during catch-up.
const FoldedOp = struct {
    kind: MetaOp.Kind,
    generation: u64, // the op's pos (== generation for a create)
};

pub fn init(allocator: std.mem.Allocator, mi: *MultiIndex, coordinator: Coordinator) Self {
    return .{ .allocator = allocator, .mi = mi, .coordinator = coordinator };
}

pub fn deinit(self: *Self) void {
    // Stop the meta consumer first so it can't create/delete more indexes while we
    // tear the data consumers down.
    if (self.meta_task) |*t| t.cancel(); // cancel + join
    self.meta_task = null;

    var it = self.consumers.iterator();
    while (it.next()) |e| {
        const c = e.value_ptr.*;
        if (c.task) |*t| t.cancel(); // cancel + join
        self.allocator.free(c.name);
        self.allocator.destroy(c);
    }
    self.consumers.deinit(self.allocator);
}

/// Start the meta consumer (called once after open(), before serving). It
/// reconciles local indexes against the meta feed and keeps them in sync. Data
/// consumers are started by the reconcile path, not here.
pub fn start(self: *Self) !void {
    self.meta_task = try zio.spawn(metaLoop, .{self});
}

/// Start a data consumer for the (`name`, `generation`) lineage beginning at
/// `start_version`. Idempotent (no-op if a consumer for `name` already runs). Safe
/// to call while the caller holds the MultiIndex lock: it only touches this
/// component's lock and spawns — it never calls back into MultiIndex.
pub fn addConsumer(self: *Self, name: []const u8, generation: u64, start_version: u64) !void {
    try self.mutex.lock();
    defer self.mutex.unlock();
    if (self.consumers.contains(name)) return;

    const name_copy = try self.allocator.dupe(u8, name);
    errdefer self.allocator.free(name_copy);
    const c = try self.allocator.create(Consumer);
    errdefer self.allocator.destroy(c);
    c.* = .{ .name = name_copy, .generation = generation, .replicator = self, .applied = start_version };
    try self.consumers.put(self.allocator, name_copy, c);
    errdefer _ = self.consumers.remove(name_copy);
    c.task = try zio.spawn(consumeLoop, .{ c, start_version });
}

/// Stop and remove the data consumer for `name`. deleteIndexLocal calls this
/// *before* it takes the MultiIndex lock, so a consumer's in-flight apply can still
/// finish (its borrow drains for the delete's wait).
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

/// Write path in replicated mode: append to the active lineage's data feed, wait
/// for the local consumer to apply up to the assigned seq (read-your-writes),
/// return that seq as the version.
pub fn update(self: *Self, name: []const u8, request: api.UpdateRequest) !api.UpdateResponse {
    const generation = blk: {
        try self.mutex.lock();
        defer self.mutex.unlock();
        const c = self.consumers.get(name) orelse return error.IndexNotFound;
        break :blk c.generation;
    };
    const version = try self.coordinator.append(name, generation, request.changes, request.expected_version);
    try self.waitApplied(name, version);
    return .{ .version = version };
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
        const n = self.coordinator.read(c.name, c.generation, after, &buf, .none) catch |err| {
            if (err == error.Canceled) return error.Canceled;
            log.warn("data read failed for '{s}' gen {d}: {}", .{ c.name, c.generation, err });
            continue;
        };
        if (n == 0) continue;

        for (buf[0..n], 0..) |e, i| changes[i] = e.change;
        const version = buf[n - 1].id; // coalesce the batch; version = max seq
        self.mi.applyLog(c.name, changes[0..n], null, version) catch |err| {
            if (err == error.Canceled) return error.Canceled;
            // Skip past the batch rather than spin on a persistently bad entry.
            log.warn("apply failed for '{s}' at version {d}: {}", .{ c.name, version, err });
        };
        after = version;
        self.markApplied(c, version);
    }
}

// ---- meta consumer ----

fn metaLoop(self: *Self) zio.Cancelable!void {
    var after: u64 = 0;

    // Phase 1: catch up and fold to final state per name, so a replica joining a
    // long-lived cluster reconciles each index once instead of replaying every
    // create/delete. readMeta returns available ops immediately and only waits the
    // window on an empty feed, so the whole history drains fast regardless.
    {
        var folded: std.StringHashMapUnmanaged(FoldedOp) = .empty;
        defer {
            var kit = folded.keyIterator();
            while (kit.next()) |k| self.allocator.free(k.*);
            folded.deinit(self.allocator);
        }
        var buf: [meta_batch]MetaOp = undefined;
        while (true) {
            const n = self.coordinator.readMeta(after, &buf, .{ .duration = .fromMilliseconds(meta_catchup_ms) }) catch |err| {
                if (err == error.Canceled) return error.Canceled;
                log.warn("meta catch-up read failed: {}", .{err});
                continue;
            };
            if (n == 0) break; // drained within the window -> caught up
            for (buf[0..n]) |op| {
                after = op.pos;
                self.foldPut(&folded, op) catch |err| {
                    if (err == error.Canceled) return error.Canceled;
                    log.warn("meta fold failed for '{s}': {}", .{ op.index_name, err });
                };
            }
        }
        var it = folded.iterator();
        while (it.next()) |e| {
            self.reconcileOne(e.key_ptr.*, e.value_ptr.kind, e.value_ptr.generation) catch |err| {
                if (err == error.Canceled) return error.Canceled;
                log.warn("meta reconcile (catch-up) failed for '{s}': {}", .{ e.key_ptr.*, err });
            };
        }
        self.markMetaApplied(after);
    }

    // Phase 2: stream, reconciling each op as it arrives (each new op is the latest
    // for its index, so per-op reconcile is safe).
    var buf: [meta_batch]MetaOp = undefined;
    while (true) {
        const n = self.coordinator.readMeta(after, &buf, .none) catch |err| {
            if (err == error.Canceled) return error.Canceled;
            log.warn("meta stream read failed: {}", .{err});
            continue;
        };
        for (buf[0..n]) |op| {
            self.reconcileOne(op.index_name, op.kind, op.pos) catch |err| {
                if (err == error.Canceled) return error.Canceled;
                log.warn("meta reconcile failed for '{s}' at pos {d}: {}", .{ op.index_name, op.pos, err });
            };
            after = op.pos;
            self.markMetaApplied(op.pos);
        }
    }
}

fn reconcileOne(self: *Self, name: []const u8, kind: MetaOp.Kind, generation: u64) !void {
    switch (kind) {
        .create => try self.mi.reconcileCreate(name, generation),
        .delete => try self.mi.deleteIndexLocal(name),
    }
}

fn foldPut(self: *Self, folded: *std.StringHashMapUnmanaged(FoldedOp), op: MetaOp) !void {
    const gop = try folded.getOrPut(self.allocator, op.index_name);
    if (!gop.found_existing) gop.key_ptr.* = try self.allocator.dupe(u8, op.index_name);
    gop.value_ptr.* = .{ .kind = op.kind, .generation = op.pos };
}

pub fn waitMetaApplied(self: *Self, pos: u64) !void {
    try self.mutex.lock();
    defer self.mutex.unlock();
    while (self.meta_applied < pos) try self.meta_cond.wait(&self.mutex);
}

fn markMetaApplied(self: *Self, pos: u64) void {
    self.mutex.lockUncancelable();
    defer self.mutex.unlock();
    if (pos > self.meta_applied) self.meta_applied = pos;
    self.meta_cond.broadcast();
}

// ---- tests ----

test "replicated create+update flows through the coordinator; RYW + search see it" {
    const MemoryCoordinator = coordinator_mod.MemoryCoordinator;
    const common = @import("common.zig");

    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_replicator_flow";
    common.deleteDirTree(std.testing.allocator, cwd, dir_path) catch {};
    try cwd.createDir(dir_path, 0o755);
    defer common.deleteDirTree(std.testing.allocator, cwd, dir_path) catch {};
    const dir = try cwd.openDir(dir_path, .{ .iterate = true });

    // The coordinator must outlive the manager (its consumers borrow it), so declare
    // it first — LIFO defers then run mi.deinit() before cl.deinit().
    var cl = MemoryCoordinator.init(std.testing.allocator);
    defer cl.deinit();

    var mi = MultiIndex.init(std.testing.allocator, dir);
    defer mi.deinit();
    try mi.startReplication(cl.coordinator());

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Create routes through the coordinator + waits for the local meta consumer to
    // build the index (create-your-writes).
    const created = try mi.createIndex("main", .{});
    try std.testing.expect(created.ready);

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

    // A second index created after replication started gets its own consumer.
    _ = try mi.createIndex("other", .{});
    var h2 = [_]u32{ 7, 8, 9 };
    const c2 = [_]Change{.{ .insert = .{ .id = 42, .hashes = &h2 } }};
    _ = try mi.update(a, "other", .{ .changes = &c2 });
    var q2 = [_]u32{ 7, 8, 9 };
    const s2 = try mi.search(a, "other", .{ .query = &q2 });
    try std.testing.expectEqual(@as(usize, 1), s2.results.len);
    try std.testing.expectEqual(@as(u32, 42), s2.results[0].id);
}

test "replicated delete+recreate converges on the new lineage" {
    const MemoryCoordinator = coordinator_mod.MemoryCoordinator;
    const common = @import("common.zig");

    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_replicator_recreate";
    common.deleteDirTree(std.testing.allocator, cwd, dir_path) catch {};
    try cwd.createDir(dir_path, 0o755);
    defer common.deleteDirTree(std.testing.allocator, cwd, dir_path) catch {};
    const dir = try cwd.openDir(dir_path, .{ .iterate = true });

    var cl = MemoryCoordinator.init(std.testing.allocator);
    defer cl.deinit();

    var mi = MultiIndex.init(std.testing.allocator, dir);
    defer mi.deinit();
    try mi.startReplication(cl.coordinator());

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const c1 = try mi.createIndex("main", .{});
    var h = [_]u32{ 1, 2, 3 };
    _ = try mi.update(a, "main", .{ .changes = &[_]Change{.{ .insert = .{ .id = 1, .hashes = &h } }} });

    // Delete, then recreate: a new, higher generation.
    _ = try mi.deleteIndex("main", .{});
    const c2 = try mi.createIndex("main", .{});
    try std.testing.expect(c2.generation > c1.generation);

    // The recreated index is empty — the old lineage's doc is gone (isolation is
    // the generation scope).
    var q = [_]u32{ 1, 2, 3 };
    const s = try mi.search(a, "main", .{ .query = &q });
    try std.testing.expectEqual(@as(usize, 0), s.results.len);

    // New writes land on the new lineage and are searchable.
    var h2 = [_]u32{ 4, 5, 6 };
    _ = try mi.update(a, "main", .{ .changes = &[_]Change{.{ .insert = .{ .id = 2, .hashes = &h2 } }} });
    var q2 = [_]u32{ 4, 5, 6 };
    const s2 = try mi.search(a, "main", .{ .query = &q2 });
    try std.testing.expectEqual(@as(usize, 1), s2.results.len);
    try std.testing.expectEqual(@as(u32, 2), s2.results[0].id);
}
