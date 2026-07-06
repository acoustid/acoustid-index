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
const meta_catchup: zio.Timeout = .{ .duration = .fromMilliseconds(100) };
// Backoff before retrying a transient failure (fold OOM, data apply, feed read), so
// a persistently-failing coordinator doesn't tight-spin.
const fold_retry: zio.Duration = .fromMilliseconds(1000);
const apply_retry: zio.Duration = .fromMilliseconds(1000);
const read_retry: zio.Duration = .fromMilliseconds(1000);
// How often the meta loop retries parked (persistently-failing) reconciles while it
// otherwise blocks waiting for new meta ops.
const pending_retry: zio.Timeout = .{ .duration = .fromMilliseconds(1000) };
// A write's read-your-writes wait gives up this long after it starts if the write
// still hasn't applied — so a coordinator that dies after accepting the write returns
// error.ReplicationTimeout (-> 503) instead of hanging the request forever.
// Overridable per-instance (`ryw_timeout` field); tests set it short.
pub const default_ryw_timeout: zio.Duration = .fromMilliseconds(30_000);

allocator: std.mem.Allocator,
mi: *MultiIndex,
coordinator: Coordinator,
mutex: zio.Mutex = .init,
cond: zio.Condition = .init, // broadcast after each data apply (read-your-writes)
meta_cond: zio.Condition = .init, // broadcast after each meta apply (create/delete-your-writes)
consumers: std.StringHashMapUnmanaged(*Consumer) = .empty,
meta_applied: u64 = 0, // max meta pos reconciled (guarded by mutex)
meta_task: ?zio.JoinHandle(zio.Cancelable!void) = null,
ryw_timeout: zio.Duration = default_ryw_timeout, // read/create/delete-your-writes budget

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
    if (self.consumers.get(name)) |existing| {
        // Idempotent per name. Callers must stop the old lineage's consumer (via
        // removeConsumer in deleteIndexLocal) before starting a new generation, so
        // an existing consumer here must be the same lineage.
        std.debug.assert(existing.generation == generation);
        return;
    }

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
pub fn update(self: *Self, name: []const u8, changes: []const Change, expected_version: ?u64) !api.UpdateResponse {
    const generation = blk: {
        try self.mutex.lock();
        defer self.mutex.unlock();
        const c = self.consumers.get(name) orelse return error.IndexNotFound;
        break :blk c.generation;
    };
    const version = try self.coordinator.append(name, generation, changes, expected_version);
    try self.waitApplied(name, generation, version);
    return .{ .version = version };
}

// Wait until this node's consumer for `name` has applied up to `id`. `generation`
// pins the wait to the lineage we appended to: if a delete+recreate swapped the
// lineage in the meantime, the write landed on a now-dead feed, so we report it as
// gone rather than falsely satisfying it against the new lineage's unrelated seqs.
fn waitApplied(self: *Self, name: []const u8, generation: u64, id: u64) !void {
    try self.mutex.lock();
    defer self.mutex.unlock();
    // Absolute deadline, fixed up front: `cond` is broadcast on EVERY lineage's
    // apply (markApplied), so a per-wait duration would be rearmed by unrelated
    // wakeups and never fire on a busy multi-index node. `.deadline` is immune.
    const deadline = self.rywDeadline();
    while (true) {
        const c = self.consumers.get(name) orelse return error.IndexNotFound;
        if (c.generation != generation) return error.IndexNotFound;
        if (c.applied >= id) return;
        self.cond.timedWait(&self.mutex, deadline) catch |err| switch (err) {
            error.Timeout => return error.ReplicationTimeout,
            else => |e| return e, // Canceled -> shutdown
        };
    }
}

// An absolute monotonic deadline `ryw_timeout` from now, for the read/create/delete-
// your-writes waits. Passing the same absolute deadline to every timedWait keeps a
// shared-condition wakeup from rearming it.
fn rywDeadline(self: *Self) zio.Timeout {
    return .{ .deadline = zio.Timestamp.now(.monotonic).addDuration(self.ryw_timeout) };
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
            try zio.sleep(read_retry);
            continue;
        };
        if (n == 0) continue;

        for (buf[0..n], 0..) |e, i| changes[i] = e.change;
        const version = buf[n - 1].id; // coalesce the batch; version = max seq
        if (!try self.applyWithRetry(c, changes[0..n], version)) return; // lineage gone
        after = version;
        self.markApplied(c, version);
    }
}

// Apply a batch, retrying transient failures with backoff so the batch is durably
// applied BEFORE we advance the watermark / mark it applied — otherwise a failed
// apply would falsely satisfy a writer's read-your-writes with a doc that never
// entered the index. Returns false if this consumer is obsolete (its lineage was
// deleted or rebuilt underneath it) and should stop.
fn applyWithRetry(self: *Self, c: *Consumer, changes: []const Change, version: u64) zio.Cancelable!bool {
    while (true) {
        self.mi.applyLog(c.name, c.generation, changes, version) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.IndexNotFound, error.IndexGenerationMismatch => {
                // Unreachable under the current invariant (deleteIndexLocal/rebuild
                // always removeConsumer — cancel+join — before dropping/replacing a
                // lineage, so a live consumer never sees its own index gone). Warn
                // loudly if it ever fires: the Consumer stays in the map (we can't
                // self-remove: removeConsumer joins this task), so a later addConsumer
                // for the same name no-ops and the lineage silently stops replicating.
                log.warn("data consumer for '{s}' gen {d} self-stopping on {s} (removeConsumer should have preceded this)", .{ c.name, c.generation, @errorName(err) });
                return false;
            },
            else => {
                log.warn("apply failed for '{s}' at version {d} (retrying): {}", .{ c.name, version, err });
                try zio.sleep(apply_retry);
                continue;
            },
        };
        return true;
    }
}

// ---- meta consumer ----

fn metaLoop(self: *Self) zio.Cancelable!void {
    var after: u64 = 0;

    // Reconciles that keep failing are parked here (by name, latest op wins) and
    // retried on a timer, so a single poison op can't wedge the whole feed. The
    // watermark still advances past a parked op — createIndexReplicated re-checks
    // the index is actually present, so a parked create surfaces to its client as
    // "not found, retry" rather than blocking every OTHER index's lifecycle.
    var pending: std.StringHashMapUnmanaged(FoldedOp) = .empty;
    defer {
        var pit = pending.keyIterator();
        while (pit.next()) |k| self.allocator.free(k.*);
        pending.deinit(self.allocator);
    }

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
            const n = self.coordinator.readMeta(after, &buf, meta_catchup) catch |err| {
                if (err == error.Canceled) return error.Canceled;
                log.warn("meta catch-up read failed: {}", .{err});
                try zio.sleep(read_retry);
                continue;
            };
            if (n == 0) break; // drained within the window -> caught up
            for (buf[0..n]) |op| {
                // Fold must be complete before the convergence step below trusts it
                // (a dropped op would look like an index the feed never mentioned),
                // so retry rather than skip. `after` advances only on success.
                try self.foldPutWithRetry(&folded, op);
                after = op.pos;
            }
        }
        // Reconcile the feed's active lineages (create/rebuild), then converge
        // deletions: drop any local index the feed doesn't list as active (a
        // leftover from a delete this node missed while down, or coordinator/disk
        // divergence — otherwise it lingers as a searchable zombie with no consumer).
        var it = folded.iterator();
        while (it.next()) |e| {
            try self.reconcileOrPark(&pending, e.key_ptr.*, e.value_ptr.kind, e.value_ptr.generation);
        }
        self.dropStaleLocalIndexes(&folded) catch |err| {
            if (err == error.Canceled) return error.Canceled;
            log.warn("failed to drop stale local indexes: {}", .{err});
        };
        self.markMetaApplied(after);
    }

    // Phase 2: stream, reconciling each op as it arrives (each new op is the latest
    // for its index, so per-op reconcile is safe). Between reads, retry parked ops;
    // while any are parked, bound the read so the retry timer keeps firing.
    var buf: [meta_batch]MetaOp = undefined;
    while (true) {
        try self.retryParked(&pending);
        const deadline: zio.Timeout = if (pending.count() == 0) .none else pending_retry;
        const n = self.coordinator.readMeta(after, &buf, deadline) catch |err| {
            if (err == error.Canceled) return error.Canceled;
            log.warn("meta stream read failed: {}", .{err});
            try zio.sleep(read_retry);
            continue;
        };
        for (buf[0..n]) |op| {
            try self.reconcileOrPark(&pending, op.index_name, op.kind, op.pos);
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

// Reconcile one op once. On a non-Canceled failure, park it by name (latest op wins)
// for the retry timer instead of blocking the loop; on success, clear any prior
// parked entry. Cancellation propagates for clean shutdown.
fn reconcileOrPark(self: *Self, pending: *std.StringHashMapUnmanaged(FoldedOp), name: []const u8, kind: MetaOp.Kind, generation: u64) zio.Cancelable!void {
    self.reconcileOne(name, kind, generation) catch |err| {
        if (err == error.Canceled) return error.Canceled;
        log.warn("meta reconcile failed for '{s}' (parking for retry): {}", .{ name, err });
        self.park(pending, name, .{ .kind = kind, .generation = generation }) catch |perr| {
            if (perr == error.Canceled) return error.Canceled;
            log.warn("failed to park meta op for '{s}' (stranded until superseded): {}", .{ name, perr });
        };
        return;
    };
    self.unpark(pending, name);
}

// Retry every parked op; drop the ones that now succeed. Only the meta loop touches
// `pending`, so iterating while reconcileOne suspends is safe.
fn retryParked(self: *Self, pending: *std.StringHashMapUnmanaged(FoldedOp)) zio.Cancelable!void {
    if (pending.count() == 0) return;
    var done: std.ArrayListUnmanaged([]const u8) = .empty;
    defer done.deinit(self.allocator);
    var it = pending.iterator();
    while (it.next()) |e| {
        self.reconcileOne(e.key_ptr.*, e.value_ptr.kind, e.value_ptr.generation) catch |err| {
            if (err == error.Canceled) return error.Canceled;
            continue; // still failing; leave it parked
        };
        // Record the key to remove after iterating (can't mutate the map mid-scan).
        done.append(self.allocator, e.key_ptr.*) catch {}; // OOM: just retry next round
    }
    for (done.items) |name| self.unpark(pending, name);
}

fn park(self: *Self, pending: *std.StringHashMapUnmanaged(FoldedOp), name: []const u8, op: FoldedOp) !void {
    const gop = try pending.getOrPut(self.allocator, name);
    if (!gop.found_existing) gop.key_ptr.* = try self.allocator.dupe(u8, name);
    gop.value_ptr.* = op;
}

fn unpark(self: *Self, pending: *std.StringHashMapUnmanaged(FoldedOp), name: []const u8) void {
    if (pending.fetchRemove(name)) |kv| self.allocator.free(kv.key);
}

fn foldPut(self: *Self, folded: *std.StringHashMapUnmanaged(FoldedOp), op: MetaOp) !void {
    const gop = try folded.getOrPut(self.allocator, op.index_name);
    if (!gop.found_existing) gop.key_ptr.* = try self.allocator.dupe(u8, op.index_name);
    gop.value_ptr.* = .{ .kind = op.kind, .generation = op.pos };
}

fn foldPutWithRetry(self: *Self, folded: *std.StringHashMapUnmanaged(FoldedOp), op: MetaOp) zio.Cancelable!void {
    while (true) {
        self.foldPut(folded, op) catch |err| {
            if (err == error.Canceled) return error.Canceled;
            log.warn("meta fold failed for '{s}' (retrying): {}", .{ op.index_name, err });
            try zio.sleep(fold_retry);
            continue;
        };
        return;
    }
}

// Drop every local index the folded feed state doesn't list as active (i.e. its
// latest meta op is not a create). Converges this node to the coordinator registry.
fn dropStaleLocalIndexes(self: *Self, folded: *const std.StringHashMapUnmanaged(FoldedOp)) !void {
    const names = try self.mi.indexNames(self.allocator);
    defer {
        for (names) |n| self.allocator.free(n);
        self.allocator.free(names);
    }
    for (names) |name| {
        if (folded.get(name)) |f| {
            if (f.kind == .create) continue; // active in the feed -> keep
        }
        log.info("dropping local index '{s}' absent from the meta feed", .{name});
        self.mi.deleteIndexLocal(name) catch |err| {
            if (err == error.Canceled) return err;
            log.warn("failed to drop stale local index '{s}': {}", .{ name, err });
        };
    }
}

pub fn waitMetaApplied(self: *Self, pos: u64) !void {
    try self.mutex.lock();
    defer self.mutex.unlock();
    const deadline = self.rywDeadline();
    while (self.meta_applied < pos) {
        self.meta_cond.timedWait(&self.mutex, deadline) catch |err| switch (err) {
            // Fires only when the meta consumer makes no progress before the deadline
            // (wedged, e.g. coordinator unreachable). A parked op advances the
            // watermark, so it never lands here.
            error.Timeout => return error.ReplicationTimeout,
            else => |e| return e, // Canceled -> shutdown
        };
    }
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

test "meta consumer drops a local index absent from the meta feed" {
    const MemoryCoordinator = coordinator_mod.MemoryCoordinator;
    const common = @import("common.zig");

    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_replicator_orphan";
    common.deleteDirTree(std.testing.allocator, cwd, dir_path) catch {};
    try cwd.createDir(dir_path, 0o755);
    defer common.deleteDirTree(std.testing.allocator, cwd, dir_path) catch {};
    const dir = try cwd.openDir(dir_path, .{ .iterate = true });

    var cl = MemoryCoordinator.init(std.testing.allocator);
    defer cl.deinit();

    var mi = MultiIndex.init(std.testing.allocator, dir);
    defer mi.deinit();

    // A local index the coordinator never learned about (standalone create, before
    // replication starts) — the zombie case.
    _ = try mi.createIndex("orphan", .{});
    try std.testing.expect(try mi.checkIndexExists("orphan"));

    // Start replication against an empty coordinator: catch-up must converge by
    // dropping the orphan (it isn't active in the meta feed).
    try mi.startReplication(cl.coordinator());

    var i: usize = 0;
    while (i < 300) : (i += 1) {
        if (!try mi.checkIndexExists("orphan")) break;
        try zio.sleep(.fromMilliseconds(10));
    }
    try std.testing.expect(!try mi.checkIndexExists("orphan"));
}

// Test coordinator: delegates everything to `inner` except data `read`, which fails
// for `only` (or every index when `only == null`) — a coordinator reachable for
// writes/meta but wedged for (some) data reads.
const WedgedReads = struct {
    inner: Coordinator,
    only: ?[]const u8 = null,

    fn coordinator(self: *WedgedReads) Coordinator {
        return .{ .ptr = self, .vtable = &vtable };
    }
    const vtable: Coordinator.VTable = .{
        .append = appendImpl,
        .read = readImpl,
        .createIndex = createIndexImpl,
        .deleteIndex = deleteIndexImpl,
        .readMeta = readMetaImpl,
    };
    fn appendImpl(ptr: *anyopaque, name: []const u8, generation: u64, changes: []const Change, expected: ?u64) anyerror!u64 {
        const self: *WedgedReads = @ptrCast(@alignCast(ptr));
        return self.inner.append(name, generation, changes, expected);
    }
    fn readImpl(ptr: *anyopaque, name: []const u8, generation: u64, after: u64, out: []Entry, deadline: zio.Timeout) anyerror!usize {
        const self: *WedgedReads = @ptrCast(@alignCast(ptr));
        if (self.only == null or std.mem.eql(u8, name, self.only.?)) return error.CoordinatorError;
        return self.inner.read(name, generation, after, out, deadline);
    }
    fn createIndexImpl(ptr: *anyopaque, name: []const u8) anyerror!u64 {
        const self: *WedgedReads = @ptrCast(@alignCast(ptr));
        return self.inner.createIndex(name);
    }
    fn deleteIndexImpl(ptr: *anyopaque, name: []const u8) anyerror!u64 {
        const self: *WedgedReads = @ptrCast(@alignCast(ptr));
        return self.inner.deleteIndex(name);
    }
    fn readMetaImpl(ptr: *anyopaque, after: u64, out: []MetaOp, deadline: zio.Timeout) anyerror!usize {
        const self: *WedgedReads = @ptrCast(@alignCast(ptr));
        return self.inner.readMeta(after, out, deadline);
    }
};

test "read-your-writes times out when the consumer can't apply" {
    const MemoryCoordinator = coordinator_mod.MemoryCoordinator;
    const common = @import("common.zig");

    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_replicator_ryw_timeout";
    common.deleteDirTree(std.testing.allocator, cwd, dir_path) catch {};
    try cwd.createDir(dir_path, 0o755);
    defer common.deleteDirTree(std.testing.allocator, cwd, dir_path) catch {};
    const dir = try cwd.openDir(dir_path, .{ .iterate = true });

    var cl = MemoryCoordinator.init(std.testing.allocator);
    defer cl.deinit();
    var wedged = WedgedReads{ .inner = cl.coordinator() };

    var mi = MultiIndex.init(std.testing.allocator, dir);
    defer mi.deinit();
    try mi.startReplication(wedged.coordinator());
    mi.replication.?.ryw_timeout = .fromMilliseconds(300);

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    _ = try mi.createIndex("main", .{}); // meta path works -> index + consumer exist

    // The append is accepted; the consumer can never read it back, so the write's
    // read-your-writes wait times out (-> 503) instead of hanging forever.
    var h = [_]u32{ 1, 2, 3 };
    const changes = [_]Change{.{ .insert = .{ .id = 1, .hashes = &h } }};
    try std.testing.expectError(error.ReplicationTimeout, mi.update(a, "main", .{ .changes = &changes }));
}

test "read-your-writes timeout is not rearmed by other lineages applying" {
    const MemoryCoordinator = coordinator_mod.MemoryCoordinator;
    const common = @import("common.zig");

    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(3) });
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_replicator_ryw_shared_cond";
    common.deleteDirTree(std.testing.allocator, cwd, dir_path) catch {};
    try cwd.createDir(dir_path, 0o755);
    defer common.deleteDirTree(std.testing.allocator, cwd, dir_path) catch {};
    const dir = try cwd.openDir(dir_path, .{ .iterate = true });

    var cl = MemoryCoordinator.init(std.testing.allocator);
    defer cl.deinit();
    var wedged = WedgedReads{ .inner = cl.coordinator(), .only = "bad" }; // only "bad" reads fail

    var mi = MultiIndex.init(std.testing.allocator, dir);
    defer mi.deinit();
    try mi.startReplication(wedged.coordinator());
    mi.replication.?.ryw_timeout = .fromMilliseconds(400);

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    _ = try mi.createIndex("good", .{});
    _ = try mi.createIndex("bad", .{});

    // Keep "good" applying in the background: markApplied broadcasts the shared cond,
    // which a per-wait timeout would treat as progress and rearm forever. The wait on
    // "bad" (wedged) must still hit its absolute deadline.
    var stop = std.atomic.Value(bool).init(false);
    const Bg = struct {
        fn run(m: *MultiIndex, s: *std.atomic.Value(bool)) void {
            var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer ar.deinit();
            var id: u32 = 1;
            while (!s.load(.acquire)) : (id += 1) {
                _ = ar.reset(.retain_capacity);
                var h = [_]u32{id};
                const c = [_]Change{.{ .insert = .{ .id = id, .hashes = &h } }};
                _ = m.update(ar.allocator(), "good", .{ .changes = &c }) catch return;
                zio.sleep(.fromMilliseconds(20)) catch return;
            }
        }
    };
    var bg = try zio.spawn(Bg.run, .{ &mi, &stop });
    defer {
        stop.store(true, .release);
        bg.cancel();
    }

    var h = [_]u32{ 7, 8, 9 };
    const changes = [_]Change{.{ .insert = .{ .id = 7, .hashes = &h } }};
    try std.testing.expectError(error.ReplicationTimeout, mi.update(a, "bad", .{ .changes = &changes }));
}

test "a poison meta op parks and does not wedge other indexes" {
    const MemoryCoordinator = coordinator_mod.MemoryCoordinator;
    const common = @import("common.zig");

    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_replicator_poison";
    common.deleteDirTree(std.testing.allocator, cwd, dir_path) catch {};
    try cwd.createDir(dir_path, 0o755);
    defer common.deleteDirTree(std.testing.allocator, cwd, dir_path) catch {};
    const dir = try cwd.openDir(dir_path, .{ .iterate = true });

    // Plant a plain FILE where index "bad"'s directory would go, so its reconcile
    // (openOrCreateDir -> openDir) fails deterministically forever.
    (try dir.createFile("bad", .{ .truncate = true })).close();

    var cl = MemoryCoordinator.init(std.testing.allocator);
    defer cl.deinit();

    var mi = MultiIndex.init(std.testing.allocator, dir);
    defer mi.deinit();
    try mi.startReplication(cl.coordinator());

    // The poison create can't reconcile; it parks, the watermark advances, and the
    // caller gets "not found" (retryable) rather than a hang.
    try std.testing.expectError(error.IndexNotFound, mi.createIndex("bad", .{}));

    // A healthy index still creates fine — the parked op did NOT block the feed.
    const good = try mi.createIndex("good", .{});
    try std.testing.expect(good.ready);
    try std.testing.expect(try mi.checkIndexExists("good"));
}

test "standalone createIndex honors the generation" {
    const common = @import("common.zig");

    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_standalone_generation";
    common.deleteDirTree(std.testing.allocator, cwd, dir_path) catch {};
    try cwd.createDir(dir_path, 0o755);
    defer common.deleteDirTree(std.testing.allocator, cwd, dir_path) catch {};
    const dir = try cwd.openDir(dir_path, .{ .iterate = true });

    var mi = MultiIndex.init(std.testing.allocator, dir);
    defer mi.deinit();

    // Create at a specific generation (not auto-assigned), same as the coordinator
    // would stamp in replicated mode.
    try std.testing.expectEqual(@as(u64, 5), (try mi.createIndex("main", .{ .generation = 5 })).generation);
    // Idempotent at the same generation; a mismatch on the active index conflicts.
    try std.testing.expectEqual(@as(u64, 5), (try mi.createIndex("main", .{ .generation = 5 })).generation);
    try std.testing.expectError(error.OlderIndexAlreadyExists, mi.createIndex("main", .{ .generation = 4 }));
    try std.testing.expectError(error.NewerIndexAlreadyExists, mi.createIndex("main", .{ .generation = 6 }));

    // After delete, a recreate must advance past the prior lineage.
    _ = try mi.deleteIndex("main", .{});
    try std.testing.expectError(error.OlderIndexAlreadyExists, mi.createIndex("main", .{ .generation = 5 }));
    try std.testing.expectEqual(@as(u64, 9), (try mi.createIndex("main", .{ .generation = 9 })).generation);

    // A null generation auto-assigns one past the prior.
    _ = try mi.deleteIndex("main", .{});
    try std.testing.expectEqual(@as(u64, 10), (try mi.createIndex("main", .{})).generation);
}

test "applyLog rejects a stale generation" {
    const common = @import("common.zig");

    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_applylog_gen";
    common.deleteDirTree(std.testing.allocator, cwd, dir_path) catch {};
    try cwd.createDir(dir_path, 0o755);
    defer common.deleteDirTree(std.testing.allocator, cwd, dir_path) catch {};
    const dir = try cwd.openDir(dir_path, .{ .iterate = true });

    // Standalone (no replication) is enough to exercise the apply-path guard.
    var mi = MultiIndex.init(std.testing.allocator, dir);
    defer mi.deinit();
    const created = try mi.createIndex("main", .{});

    var h = [_]u32{ 1, 2, 3 };
    const changes = [_]Change{.{ .insert = .{ .id = 1, .hashes = &h } }};
    // The current generation applies; a stale one (an older lineage's consumer that
    // should have been stopped) is rejected rather than misapplied.
    try mi.applyLog("main", created.generation, &changes, 1);
    try std.testing.expectError(error.IndexGenerationMismatch, mi.applyLog("main", created.generation + 1, &changes, 2));
}

test "replicated metadata update propagates through the coordinator" {
    const MemoryCoordinator = coordinator_mod.MemoryCoordinator;
    const Metadata = @import("change.zig").Metadata;
    const common = @import("common.zig");

    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_replicator_meta";
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

    _ = try mi.createIndex("main", .{});

    // Metadata rides the op stream: a metadata-only update goes through the log and
    // is applied by the consumer (read-your-writes), then shows up in index info.
    var md = Metadata.initOwned(std.testing.allocator);
    defer md.deinit();
    try md.set("foo", "bar");
    try md.set("rev", "7");
    _ = try mi.update(a, "main", .{ .changes = &[_]Change{}, .metadata = md });

    const info = try mi.getIndexInfo(a, "main");
    try std.testing.expectEqualStrings("bar", info.metadata.get("foo").?);
    try std.testing.expectEqualStrings("7", info.metadata.get("rev").?);
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
