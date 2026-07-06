// Manages a set of named indexes, each stored in its own subdirectory of the
// data root (the directory's presence is the index's existence). Exposes the
// high-level operations the HTTP handlers call.
//
// The manager lock is held only briefly — to look up an index and bump its
// refcount (getIndex), or to release it (releaseIndex) — never for the duration
// of a search/update. So a slow operation doesn't block createIndex/deleteIndex.
// deleteIndex marks the index and waits for outstanding borrows to drain before
// freeing it; the segment snapshot a search holds is refcounted separately, so a
// search survives a concurrent delete.

const std = @import("std");
const zio = @import("zio");
const api = @import("api.zig");
const Index = @import("Index.zig");
const Change = @import("change.zig").Change;
const Metadata = @import("change.zig").Metadata;
const MetadataEntry = @import("change.zig").MetadataEntry;
const Replicator = @import("Replicator.zig");
const Coordinator = @import("Coordinator.zig").Coordinator;
const index_redirect = @import("index_redirect.zig");
const deleteDirTree = @import("common.zig").deleteDirTree;
const SearchResults = @import("common.zig").SearchResults;
const metrics = @import("metrics.zig");
const log = std.log.scoped(.multi_index);

const Self = @This();

// A refcounted index slot. `index` is embedded, so &ref.index is stable while the
// ref lives on the heap (the maintenance coroutine captures it). `references`
// starts at 1 for the map's own reference; getIndex/releaseIndex adjust it under
// the manager lock, and deleteIndex waits for it to fall back to 1.
const IndexRef = struct {
    index: Index,
    generation: u64, // this index's lineage; persisted in the redirect + v<gen> dir
    references: usize = 1,
    being_deleted: bool = false,
    released: zio.Condition = .init,
};

fn openOrCreateDir(parent: zio.Dir, name: []const u8) !zio.Dir {
    parent.createDir(name, 0o755) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };
    return parent.openDir(name, .{ .iterate = true });
}

allocator: std.mem.Allocator,
dir: zio.Dir,
lock: zio.Mutex = .init,
indexes: std.StringHashMapUnmanaged(*IndexRef) = .empty,
checkpoint_threshold: usize = 100_000,
// Whether index oplogs fsync each append (false when an upstream owns durability).
sync: bool = true,
// Max file-segment loads in flight across all indexes during open(); 0 = no limit.
load_concurrency: usize = 0,
// Set in replicated mode: writes go through the log, a consumer applies them.
replication: ?*Replicator = null,

pub fn init(allocator: std.mem.Allocator, dir: zio.Dir) Self {
    return .{ .allocator = allocator, .dir = dir };
}

/// Enter replicated mode: writes append to `changelog` and a per-index consumer
/// applies them back. Call after open(), before serving. Borrows the changelog.
pub fn startReplication(self: *Self, coordinator: Coordinator) !void {
    const repl = try self.allocator.create(Replicator);
    errdefer self.allocator.destroy(repl);
    repl.* = Replicator.init(self.allocator, self, coordinator);
    errdefer repl.deinit();
    try repl.start();
    self.replication = repl;
}

pub fn deinit(self: *Self) void {
    // Stop consumers before tearing down the indexes they apply to.
    if (self.replication) |repl| {
        repl.deinit();
        self.allocator.destroy(repl);
        self.replication = null;
    }
    var it = self.indexes.iterator();
    while (it.next()) |entry| {
        entry.value_ptr.*.index.deinit();
        self.allocator.destroy(entry.value_ptr.*);
        self.allocator.free(entry.key_ptr.*);
    }
    self.indexes.deinit(self.allocator);
    self.dir.close();
}

// Borrow an index: bump its refcount under a brief lock and return it. The caller
// operates without holding the manager lock and must releaseIndex when done.
fn getIndex(self: *Self, name: []const u8) !*Index {
    try self.lock.lock();
    defer self.lock.unlock();
    const ref = self.indexes.get(name) orelse return error.IndexNotFound;
    if (ref.being_deleted) return error.IndexNotFound;
    ref.references += 1;
    return &ref.index;
}

// Like getIndex, but rejects if the current lineage isn't `generation` — so a data
// consumer never applies its lineage's ops to an index that was rebuilt to a newer
// generation underneath it (the analog of the old cluster's expect_generation).
fn getIndexForGeneration(self: *Self, name: []const u8, generation: u64) !*Index {
    try self.lock.lock();
    defer self.lock.unlock();
    const ref = self.indexes.get(name) orelse return error.IndexNotFound;
    if (ref.being_deleted) return error.IndexNotFound;
    if (ref.generation != generation) return error.IndexGenerationMismatch;
    ref.references += 1;
    return &ref.index;
}

// Return a borrow. Uncancelable: it runs in defer cleanup and must always
// complete (a leaked reference would deadlock deleteIndex forever).
fn releaseIndex(self: *Self, index: *Index) void {
    self.lock.lockUncancelable();
    defer self.lock.unlock();
    const ref: *IndexRef = @fieldParentPtr("index", index);
    ref.references -= 1;
    ref.released.broadcast();
}

/// Discover existing indexes (subdirectories of the data root) and open them
/// concurrently, rebuilding each from its oplog + file segments. A shared
/// semaphore (when load_concurrency > 0) caps the total file-segment loads in
/// flight across all indexes.
pub fn open(self: *Self) !void {
    // 1. Collect index names first: entry.name is only valid until the next
    //    it.next(), so we can't hold it across the concurrent opens.
    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer names.deinit(self.allocator);
    var names_owned = true;
    errdefer if (names_owned) for (names.items) |n| self.allocator.free(n);

    var it = self.dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind != .directory) continue;
        if (!isValidName(entry.name)) {
            log.warn("skipping unexpected entry '{s}' in data dir", .{entry.name});
            continue;
        }
        try names.append(self.allocator, try self.allocator.dupe(u8, entry.name));
    }
    if (names.items.len == 0) return;

    // 2. Open all indexes concurrently. Each writes its own slot; the shared load
    //    semaphore bounds concurrent segment reads (acquired per-segment inside
    //    Index.open, so index opens never hold a permit while waiting -> no
    //    deadlock).
    const refs = try self.allocator.alloc(*IndexRef, names.items.len);
    defer self.allocator.free(refs);
    const results = try self.allocator.alloc(anyerror!void, names.items.len);
    defer self.allocator.free(results);
    for (results) |*r| r.* = error.IndexNotOpened; // sentinel for slots never spawned

    var load_sem: zio.Semaphore = .{ .permits = self.load_concurrency };
    const sem_ptr: ?*zio.Semaphore = if (self.load_concurrency == 0) null else &load_sem;

    var fatal: ?anyerror = null;
    {
        var group: zio.Group = .init;
        defer group.cancel();
        for (names.items, 0..) |name, i| {
            group.spawn(openOneIndex, .{ self, name, sem_ptr, &refs[i], &results[i] }) catch |err| {
                fatal = err;
                break;
            };
        }
        group.wait() catch |err| {
            fatal = fatal orelse err;
        };
    }

    // 3. Install the indexes that opened (sequential; the map isn't concurrent),
    //    free the names of the rest. On any failure return the error — already
    //    installed indexes are torn down by deinit().
    names_owned = false; // this phase now owns every name
    for (results, names.items, 0..) |res, name, i| {
        if (res) |_| {
            const ref = refs[i];
            self.indexes.put(self.allocator, name, ref) catch |err| {
                ref.index.deinit();
                self.allocator.destroy(ref);
                self.allocator.free(name);
                fatal = fatal orelse err;
                continue;
            };
            log.info("opened index '{s}' at version {d}", .{ name, ref.index.version });
        } else |err| {
            self.allocator.free(name);
            // IndexNotOpened = never spawned; IndexSkipped = deleted/no-redirect dir.
            if (err != error.IndexNotOpened and err != error.IndexSkipped) fatal = fatal orelse err;
        }
    }
    if (fatal) |err| return err;
}

// Group-spawned: opens one index into out_ref, capturing its result into out_res
// (void return keeps the group's error flags clean).
fn openOneIndex(self: *Self, name: []const u8, load_sem: ?*zio.Semaphore, out_ref: **IndexRef, out_res: *anyerror!void) void {
    out_res.* = self.openOneIndexInner(name, load_sem, out_ref);
}

fn openOneIndexInner(self: *Self, name: []const u8, load_sem: ?*zio.Semaphore, out_ref: **IndexRef) !void {
    const name_dir = try self.dir.openDir(name, .{ .iterate = true });
    var name_dir_open = true;
    defer if (name_dir_open) name_dir.close();

    const redirect = index_redirect.read(name_dir, self.allocator) catch |err| switch (err) {
        error.FileNotFound => return error.IndexSkipped, // no redirect -> not a live index dir
        else => return err,
    };
    defer self.allocator.free(redirect.name);
    if (redirect.deleted) return error.IndexSkipped;

    var buf: [index_redirect.max_data_dir_len]u8 = undefined;
    const data_dir = try name_dir.openDir(redirect.dataDir(&buf), .{ .iterate = true });
    name_dir.close();
    name_dir_open = false;

    const ref = try self.allocator.create(IndexRef);
    errdefer self.allocator.destroy(ref);
    ref.* = .{ .index = undefined, .generation = redirect.generation };
    ref.index = Index.open(self.allocator, data_dir, self.checkpoint_threshold, self.sync, load_sem) catch |err| {
        data_dir.close();
        return err;
    };
    errdefer ref.index.deinit();
    try ref.index.start();
    out_ref.* = ref;
}

/// Search an index. Options ride in the request (limit, min_score, score_pct,
/// timeout); callers sanitize untrusted values first. `timeout == 0` = no bound.
pub fn search(self: *Self, arena: std.mem.Allocator, name: []const u8, request: api.SearchRequest) !api.SearchResponse {
    const index = try self.getIndex(name);
    defer self.releaseIndex(index);
    metrics.incSearches();

    var collector = SearchResults.init(arena, .{
        .max_results = request.limit,
        .min_score = request.min_score orelse @intCast((request.query.len + 19) / 20),
        .min_score_pct = request.score_pct,
    });
    var reader = try index.acquireReader();
    defer reader.deinit();

    // The segment scans hit maybeYield, so an expired timer cancels the task
    // there. check() tells our timeout apart from a real (shutdown) cancellation,
    // which still propagates.
    var deadline: zio.AutoCancel = .init;
    if (request.timeout != 0) deadline.set(.fromMilliseconds(request.timeout));
    defer deadline.clear();

    var sw = zio.Stopwatch.start();
    reader.search(request.query, &collector) catch |err| {
        if (err == error.Canceled and deadline.check(error.Canceled)) return error.SearchTimeout;
        return err;
    };
    metrics.observeSearchSeconds(@as(f64, @floatFromInt(sw.read().toNanoseconds())) / 1_000_000_000.0);

    const results = collector.getResults();
    if (results.len > 0) metrics.incSearchHit() else metrics.incSearchMiss();
    const out = try arena.alloc(api.SearchResult, results.len);
    for (results, 0..) |r, i| out[i] = .{ .id = r.id, .score = r.score };
    return .{ .results = out };
}

pub fn update(self: *Self, arena: std.mem.Allocator, name: []const u8, request: api.UpdateRequest) !api.UpdateResponse {
    // Fold any metadata into the change stream (as a trailing set_metadata op) once,
    // so both the replicated and local paths carry it identically through the log.
    const changes = try foldMetadata(arena, request.changes, request.metadata);

    // Replicated mode: the write goes to the log; the consumer applies it.
    if (self.replication) |repl| return repl.update(name, changes, request.expected_version);

    const index = try self.getIndex(name);
    defer self.releaseIndex(index);
    metrics.incUpdates();
    const version = try index.update(changes, .{ .expected_version = request.expected_version });
    return .{ .version = version };
}

// Append a trailing set_metadata op when metadata is present, so metadata rides the
// op stream instead of a side field (see change.zig). Entries and `changes`'
// elements are borrowed (arena-lived); the coordinator deep-copies on append.
fn foldMetadata(arena: std.mem.Allocator, changes: []const Change, metadata: ?Metadata) ![]const Change {
    const md = metadata orelse return changes;
    if (md.count() == 0) return changes;
    const entries = try arena.alloc(MetadataEntry, md.count());
    var it = md.entries.iterator();
    var i: usize = 0;
    while (it.next()) |e| : (i += 1) entries[i] = .{ .key = e.key_ptr.*, .value = e.value_ptr.* };
    const out = try arena.alloc(Change, changes.len + 1);
    @memcpy(out[0..changes.len], changes);
    out[changes.len] = .{ .set_metadata = .{ .entries = entries } };
    return out;
}

/// Apply changes at an externally-assigned version (the replicated consumer's
/// apply path; version = the lineage's per-feed seq). `generation` guards against
/// applying to a lineage that was rebuilt underneath the consumer. The external log
/// owns ordering and durability, so this just stamps the version onto the local
/// oplog + segments.
pub fn applyLog(self: *Self, name: []const u8, generation: u64, changes: []const Change, version: u64) !void {
    const index = try self.getIndexForGeneration(name, generation);
    defer self.releaseIndex(index);
    metrics.incUpdates();
    _ = try index.update(changes, .{ .version = version });
}

/// Render metrics (global counters + a per-index docs gauge) in Prometheus text.
/// Holds the manager lock across the (brief) scrape.
pub fn writeMetrics(self: *Self, w: *std.Io.Writer) !void {
    try metrics.writeGlobal(w);

    try self.lock.lock();
    defer self.lock.unlock();

    try w.writeAll("# HELP fpindex_docs Number of documents in an index\n# TYPE fpindex_docs gauge\n");
    var it = self.indexes.iterator();
    while (it.next()) |entry| {
        var reader = try entry.value_ptr.*.index.acquireReader();
        defer reader.deinit();
        try w.print("fpindex_docs{{index=\"{s}\"}} {d}\n", .{ entry.key_ptr.*, reader.numDocs() });
    }
}

pub fn checkIndexExists(self: *Self, name: []const u8) !bool {
    try self.lock.lock();
    defer self.lock.unlock();
    const ref = self.indexes.get(name) orelse return false;
    return !ref.being_deleted;
}

/// Snapshot of the current local index names (owned: caller frees each string and
/// the slice). Used by the meta consumer to converge deletions — drop any local
/// index the coordinator's registry no longer lists.
pub fn indexNames(self: *Self, allocator: std.mem.Allocator) ![][]const u8 {
    try self.lock.lock();
    defer self.lock.unlock();
    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer {
        for (names.items) |n| allocator.free(n);
        names.deinit(allocator);
    }
    var it = self.indexes.keyIterator();
    while (it.next()) |k| try names.append(allocator, try allocator.dupe(u8, k.*));
    return names.toOwnedSlice(allocator);
}

pub fn createIndex(self: *Self, name: []const u8, request: api.CreateIndexRequest) !api.CreateIndexResponse {
    if (!isValidName(name)) return error.InvalidIndexName;
    if (self.replication) |repl| return self.createIndexReplicated(repl, name, request);

    try self.lock.lock();
    defer self.lock.unlock();

    // Already active: idempotent, but honor an optimistic `generation` — a mismatch
    // is a conflict, same as the old version and as a caller can rely on in replicated
    // mode. This keeps a `generation`-supplied create binary-compatible across modes.
    if (self.indexes.get(name)) |existing| {
        if (!existing.being_deleted) {
            if (request.expect_does_not_exist) return error.IndexAlreadyExists;
            if (request.generation) |g| {
                if (g < existing.generation) return error.OlderIndexAlreadyExists;
                if (g > existing.generation) return error.NewerIndexAlreadyExists;
            }
            return .{ .version = existing.index.version, .ready = true, .generation = existing.generation };
        }
        return error.IndexAlreadyExists;
    }

    // Not active. The last lineage's generation (from the redirect), if any.
    const prior: ?u64 = blk: {
        const name_dir = openOrCreateDir(self.dir, name) catch |err| return err;
        defer name_dir.close();
        const r = index_redirect.read(name_dir, self.allocator) catch |err| switch (err) {
            error.FileNotFound => break :blk null,
            else => return err,
        };
        defer self.allocator.free(r.name);
        break :blk r.generation;
    };

    // Generation to stamp: caller-supplied (must advance past any prior lineage — the
    // same always-increasing rule the coordinator enforces in replicated mode), else
    // auto-assigned one past the prior. Either way it lands in the redirect + v<gen>
    // dir identically to how the replicated path stamps the coordinator's generation.
    const generation: u64 = if (request.generation) |g| gen: {
        if (prior) |p| {
            if (g <= p) return error.OlderIndexAlreadyExists;
        }
        break :gen g;
    } else if (prior) |p| p + 1 else 1;

    const ref = try self.installNewLineage(name, generation);
    return .{ .version = ref.index.version, .ready = true, .generation = generation };
}

// Replicated create: route through the coordinator (which mints the index_id ==
// generation on the global meta feed), then wait for THIS node's meta consumer to
// apply it (create-your-writes). Other nodes converge asynchronously via their own
// meta consumers.
fn createIndexReplicated(self: *Self, repl: *Replicator, name: []const u8, request: api.CreateIndexRequest) !api.CreateIndexResponse {
    // The coordinator owns generation assignment in replicated mode (generation = the
    // create's meta-feed position, so lineages order consistently across nodes), so a
    // client-supplied one is meaningless here — reject it rather than silently ignore.
    if (request.generation != null) return error.GenerationNotAllowed;

    // Best-effort local guard (a truly authoritative check would need the
    // coordinator to reject; createIndex is idempotent there).
    if (request.expect_does_not_exist and try self.checkIndexExists(name)) return error.IndexAlreadyExists;

    const generation = try repl.coordinator.createIndex(name);
    try repl.waitMetaApplied(generation);

    try self.lock.lock();
    defer self.lock.unlock();
    const ref = self.indexes.get(name) orelse return error.IndexNotFound;
    return .{ .version = ref.index.version, .ready = true, .generation = ref.generation };
}

// Create a fresh local lineage at `generation`, open its index, and install it in
// the map. Caller must hold the manager lock. Does not touch replication.
fn installNewLineage(self: *Self, name: []const u8, generation: u64) !*IndexRef {
    const name_dir = try openOrCreateDir(self.dir, name);
    var name_dir_open = true;
    defer if (name_dir_open) name_dir.close();

    const data_dir = try self.createLineageDir(name_dir, name, generation);
    name_dir.close();
    name_dir_open = false;

    const ref = try self.allocator.create(IndexRef);
    errdefer self.allocator.destroy(ref);
    ref.* = .{ .index = undefined, .generation = generation };
    ref.index = Index.open(self.allocator, data_dir, self.checkpoint_threshold, self.sync, null) catch |err| {
        data_dir.close();
        return err;
    };
    errdefer ref.index.deinit();
    try ref.index.start();

    const name_copy = try self.allocator.dupe(u8, name);
    errdefer self.allocator.free(name_copy);
    try self.indexes.put(self.allocator, name_copy, ref);
    return ref;
}

// Write the redirect and open (creating) the generation's v<gen> data dir. The
// returned dir has its own fd, so the caller may close name_dir afterward.
fn createLineageDir(self: *Self, name_dir: zio.Dir, name: []const u8, generation: u64) !zio.Dir {
    try index_redirect.write(name_dir, self.allocator, .{ .name = name, .generation = generation, .deleted = false });
    var buf: [index_redirect.max_data_dir_len]u8 = undefined;
    const dd = (index_redirect.IndexRedirect{ .name = name, .generation = generation }).dataDir(&buf);
    return openOrCreateDir(name_dir, dd);
}

// ---- meta-consumer local ops (replicated mode) ----
//
// Driven by the Replicator's single meta consumer; `generation` is the coordinator
// meta pos of the lineage's create. The meta consumer is the ONLY mutator of the
// index map in replicated mode, so these don't race the create/delete API (which
// routes through the coordinator and waits).

/// Reconcile the local state for `name` against a create for `generation`. Same
/// generation -> just (idempotently) ensure the data consumer runs. Different
/// generation or absent -> drop any stale lineage and build the new one.
pub fn reconcileCreate(self: *Self, name: []const u8, generation: u64) !void {
    {
        try self.lock.lock();
        defer self.lock.unlock();
        if (self.indexes.get(name)) |ref| {
            if (!ref.being_deleted and ref.generation == generation) {
                if (self.replication) |repl| try repl.addConsumer(name, generation, ref.index.version);
                return;
            }
        }
    }
    try self.deleteIndexLocal(name); // no-op if absent; drops a stale lineage
    try self.createIndexLocal(name, generation);
}

// Build a fresh local lineage at `generation` and start its data consumer. Assumes
// `name` is not currently active (reconcileCreate drops a stale lineage first).
fn createIndexLocal(self: *Self, name: []const u8, generation: u64) !void {
    try self.lock.lock();
    defer self.lock.unlock();
    const ref = try self.installNewLineage(name, generation);
    // A fresh lineage resumes its data feed at version 0 (the generation scope
    // isolates it from prior lineages — no start_position). Safe under the manager
    // lock: addConsumer only touches the Replicator lock + spawns.
    if (self.replication) |repl| try repl.addConsumer(name, generation, ref.index.version);
}

/// Drop the local index for `name` (stop its data consumer, drain borrows, remove
/// the lineage dir). No-op if absent. The meta consumer's delete + rebuild path.
pub fn deleteIndexLocal(self: *Self, name: []const u8) !void {
    // Stop the consumer first (before the manager lock) so its in-flight apply can
    // finish and its borrow drains for the wait in dropIndex.
    if (self.replication) |repl| repl.removeConsumer(name);
    _ = try self.dropIndex(name);
}

const DropResult = enum { dropped, absent };

// Remove `name` from the map: block new borrows, drain outstanding ones, deinit,
// and mark the redirect deleted (dropping the generation's data dir). Caller must
// have already stopped any replication consumer.
fn dropIndex(self: *Self, name: []const u8) !DropResult {
    try self.lock.lock();
    defer self.lock.unlock();

    const ref = self.indexes.get(name) orelse return .absent;
    if (ref.being_deleted) return .absent;

    // Block new borrows, then wait for outstanding ones to drain to the map's own
    // reference. releaseIndex broadcasts on each drop; wait releases the lock.
    ref.being_deleted = true;
    while (ref.references > 1) {
        ref.released.wait(&self.lock) catch |err| {
            ref.being_deleted = false; // a cancelled delete must not disable the index
            ref.released.broadcast();
            return err;
        };
    }

    const kv = self.indexes.fetchRemove(name).?;
    const gen = kv.value.generation;
    kv.value.index.deinit();
    self.allocator.destroy(kv.value);
    self.allocator.free(kv.key);
    // Mark the redirect deleted and drop the generation's data dir; keep
    // data/<name>/ + current so a recreate can bump to the next generation.
    self.markDeleted(name, gen) catch |err| {
        log.warn("failed to mark index '{s}' deleted: {}", .{ name, err });
    };
    return .dropped;
}

pub fn deleteIndex(self: *Self, name: []const u8, request: api.DeleteIndexRequest) !api.DeleteIndexResponse {
    if (self.replication) |repl| return self.deleteIndexReplicated(repl, name, request);

    switch (try self.dropIndex(name)) {
        .dropped => return .{ .deleted = true },
        .absent => {
            if (request.expect_exists) return error.IndexNotFound;
            return .{ .deleted = false };
        },
    }
}

// Replicated delete: route through the coordinator (append delete to the meta
// feed, get its pos), then wait for THIS node's meta consumer to apply it.
fn deleteIndexReplicated(self: *Self, repl: *Replicator, name: []const u8, request: api.DeleteIndexRequest) !api.DeleteIndexResponse {
    const existed = try self.checkIndexExists(name);
    if (!existed and request.expect_exists) return error.IndexNotFound;
    const pos = try repl.coordinator.deleteIndex(name);
    try repl.waitMetaApplied(pos);
    return .{ .deleted = existed };
}

fn markDeleted(self: *Self, name: []const u8, generation: u64) !void {
    const name_dir = try self.dir.openDir(name, .{ .iterate = true });
    defer name_dir.close();
    try index_redirect.write(name_dir, self.allocator, .{ .name = name, .generation = generation, .deleted = true });
    var buf: [index_redirect.max_data_dir_len]u8 = undefined;
    const dd = (index_redirect.IndexRedirect{ .name = name, .generation = generation }).dataDir(&buf);
    deleteDirTree(self.allocator, name_dir, dd) catch |err| {
        log.warn("failed to remove data dir for '{s}': {}", .{ name, err });
    };
}

pub fn getIndexInfo(self: *Self, arena: std.mem.Allocator, name: []const u8) !api.GetIndexInfoResponse {
    const index = try self.getIndex(name);
    defer self.releaseIndex(index);

    var reader = try index.acquireReader();
    defer reader.deinit();

    return .{
        .version = reader.version(),
        .metadata = try reader.buildMetadata(arena),
        .stats = .{
            .min_doc_id = reader.minDocId(),
            .max_doc_id = reader.maxDocId(),
            .num_segments = reader.numSegments(),
            .num_docs = reader.numDocs(),
        },
    };
}

pub fn getFingerprintInfo(self: *Self, arena: std.mem.Allocator, name: []const u8, id: u32) !api.GetFingerprintInfoResponse {
    _ = arena;
    const index = try self.getIndex(name);
    defer self.releaseIndex(index);

    var reader = try index.acquireReader();
    defer reader.deinit();

    const info = reader.getDocInfo(id) orelse return error.FingerprintNotFound;
    if (info.deleted) return error.FingerprintNotFound;
    return .{ .version = info.version };
}

pub fn checkFingerprintExists(self: *Self, name: []const u8, id: u32) !bool {
    const index = try self.getIndex(name);
    defer self.releaseIndex(index);

    var reader = try index.acquireReader();
    defer reader.deinit();

    const info = reader.getDocInfo(id);
    return info != null and !info.?.deleted;
}

/// Index names double as directory names, so restrict them to a safe set (no
/// path separators, no dots -> no traversal).
fn isValidName(name: []const u8) bool {
    if (name.len == 0 or name.len > 255) return false;
    for (name) |c| {
        if (!std.ascii.isAlphanumeric(c) and c != '_' and c != '-') return false;
    }
    return true;
}
