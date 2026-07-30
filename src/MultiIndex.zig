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
const snapshot = @import("snapshot.zig");
const Change = @import("change.zig").Change;
const Metadata = @import("change.zig").Metadata;
const MetadataEntry = @import("change.zig").MetadataEntry;
const Replicator = @import("Replicator.zig");
const Coordinator = @import("Coordinator.zig").Coordinator;
const BootstrapStream = @import("Coordinator.zig").BootstrapStream;
const index_redirect = @import("index_redirect.zig");
const deleteDirTree = @import("common.zig").deleteDirTree;
const SearchResultsPool = @import("common.zig").SearchResultsPool;
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
// Reused per-search result collectors (hit map + result list), pooled to keep
// their capacity hot across queries. See common.SearchResultsPool.
results_pool: SearchResultsPool,
indexes: std.StringHashMapUnmanaged(*IndexRef) = .empty,
checkpoint_threshold: usize = 100_000,
// Force a checkpoint once the oldest uncheckpointed write is this old (see Index);
// null disables it. Default 60s.
checkpoint_age: ?zio.Duration = .fromMilliseconds(60_000),
// Whether index oplogs fsync each append (false when an upstream owns durability).
sync: bool = true,
// Max file-segment loads in flight across all indexes during open(); 0 = no limit.
load_concurrency: usize = 0,
// Set in replicated mode: writes go through the log, a consumer applies them.
replication: ?*Replicator = null,

pub fn init(allocator: std.mem.Allocator, dir: zio.Dir) Self {
    return .{ .allocator = allocator, .dir = dir, .results_pool = SearchResultsPool.init(allocator) };
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

/// This node's lineage watermarks for one index, as served to peers looking for a
/// snapshot donor (GET /:index/_status). A single pinned reader gives a consistent
/// version/file_version pair.
pub fn getPeerStatus(self: *Self, name: []const u8) !api.PeerStatusResponse {
    const index = try self.getIndex(name);
    defer self.releaseIndex(index);
    const ref: *IndexRef = @fieldParentPtr("index", index);

    var reader = try index.acquireReader();
    defer reader.deinit();

    return .{
        .generation = ref.generation,
        .version = reader.snapshot.value.version,
        .file_version = reader.snapshot.value.file_version,
    };
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
    self.results_pool.deinit();
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

pub const SnapshotSource = struct {
    reader: Index.IndexReader,
    generation: u64,
};

// Acquire a consistent, pinned snapshot of `name` plus its lineage generation, for
// streaming out via snapshot.writeSnapshot. The returned reader pins the file segments
// independent of the index, so the caller can stream them without holding an index
// reference (and even if the index is deleted meanwhile). Caller must reader.deinit().
pub fn acquireSnapshot(self: *Self, name: []const u8) !SnapshotSource {
    const index = try self.getIndex(name);
    defer self.releaseIndex(index);
    const ref: *IndexRef = @fieldParentPtr("index", index);
    return .{ .reader = try index.acquireReader(), .generation = ref.generation };
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
    // 0. Start the search-collector sweeper (deinit cancels it). Before the early
    //    return below, so an empty data dir still gets one.
    try self.results_pool.start();

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
    ref.index.checkpoint_age = self.checkpoint_age;
    try ref.index.start();
    out_ref.* = ref;
}

/// Search an index. Options ride in the request (limit, min_score, score_pct,
/// timeout); callers sanitize untrusted values first. `timeout == 0` = no bound.
pub fn search(self: *Self, arena: std.mem.Allocator, name: []const u8, request: api.SearchRequest) !api.SearchResponse {
    const index = try self.getIndex(name);
    defer self.releaseIndex(index);

    // While a bootstrap fills this index (initial seed or below-retention
    // restore), refuse rather than answer: every result would be an
    // honest-looking but empty or stale set. IndexNotReady -> 503, the same
    // signal the health probe gives, so a caller that skipped the probe still
    // cannot mistake a loading node for a miss. After getIndex, so a missing
    // index keeps answering 404 rather than 503.
    if (self.replication) |repl| {
        if (repl.isBootstrapping(name)) return error.IndexNotReady;
    }
    metrics.incSearches();

    const collector = try self.results_pool.acquire(.{
        .max_results = request.limit,
        .min_score = request.min_score orelse @intCast((request.query.len + 19) / 20),
        .min_score_pct = request.score_pct,
    });
    defer self.results_pool.release(collector);
    var reader = try index.acquireReader();
    defer reader.deinit();

    // The segment scans hit maybeYield, so an expired timer cancels the task
    // there. check() tells our timeout apart from a real (shutdown) cancellation,
    // which still propagates.
    var deadline: zio.AutoCancel = .init;
    if (request.timeout != 0) deadline.set(.fromMilliseconds(request.timeout));
    defer deadline.clear();

    var sw = zio.Stopwatch.start();
    reader.search(request.query, collector) catch |err| {
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
    // Fingerprint id 0 is reserved: `min_doc_id == 0` is the "unset" sentinel in
    // segments (a doc with id 0 would be invisible to minDocId and skew the delta
    // base), so reject it at the ingest boundary rather than corrupt a segment.
    for (request.changes) |change| {
        const id = switch (change) {
            .insert => |op| op.id,
            .delete => |op| op.id,
            .set_metadata => continue,
        };
        if (id == 0) return error.InvalidFingerprintId;
    }

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
/// apply path; `version` = the lineage's per-feed seq). `generation` guards
/// against applying to a lineage that was rebuilt underneath the consumer. The
/// external log owns ordering and durability, so this just records the position
/// against a locally-minted commit.
///
/// Note a batch may coalesce many feed entries into one commit, which is exactly why
/// the position cannot double as the commit id.
pub fn applyLog(self: *Self, name: []const u8, generation: u64, changes: []const Change, version: u64) !void {
    const index = try self.getIndexForGeneration(name, generation);
    defer self.releaseIndex(index);
    metrics.incUpdates();
    _ = try index.update(changes, .{ .version = version });
}

/// Render metrics (global counters + per-index gauges) in Prometheus text.
/// Holds the manager lock across the (brief) scrape.
pub fn writeMetrics(self: *Self, w: *std.Io.Writer) !void {
    try metrics.writeGlobal(w);

    try self.lock.lock();
    defer self.lock.unlock();

    // One pass per family: the text format wants every sample of a family under
    // a single HELP/TYPE, so the families cannot be interleaved per index. The
    // lock is held throughout, so the set of indexes is the same in each pass.
    try w.writeAll("# HELP fpindex_docs Number of documents in an index\n# TYPE fpindex_docs gauge\n");
    var docs_it = self.indexes.iterator();
    while (docs_it.next()) |entry| {
        var reader = try entry.value_ptr.*.index.acquireReader();
        defer reader.deinit();
        try w.print("fpindex_docs{{index=\"{s}\"}} {d}\n", .{ entry.key_ptr.*, reader.numDocs() });
    }

    try w.writeAll("# HELP fpindex_version Last changelog position applied to an index\n# TYPE fpindex_version gauge\n");
    var version_it = self.indexes.iterator();
    while (version_it.next()) |entry| {
        var reader = try entry.value_ptr.*.index.acquireReader();
        defer reader.deinit();
        try w.print("fpindex_version{{index=\"{s}\"}} {d}\n", .{ entry.key_ptr.*, reader.version() });
    }
}

pub fn checkIndexExists(self: *Self, name: []const u8) !bool {
    try self.lock.lock();
    defer self.lock.unlock();
    const ref = self.indexes.get(name) orelse return false;
    return !ref.being_deleted;
}

pub const IndexHealth = enum { ready, loading, missing };

/// Per-index health: `loading` while the index exists but its consumer is filling
/// it by bootstrap — the initial empty-lineage seed or a below-retention restore.
/// Either way its answers would be honest-looking but empty or stale, so a search
/// balancer must be able to tell. Global liveness (/_health) deliberately stays
/// independent of this: peer discovery must keep finding nodes that are
/// open-but-loading, or a cold cluster start deadlocks — nobody is ready, DNS is
/// empty, nobody can find a donor, nobody becomes ready.
pub fn indexHealth(self: *Self, name: []const u8) !IndexHealth {
    try self.lock.lock();
    defer self.lock.unlock();
    const ref = self.indexes.get(name) orelse return .missing;
    if (ref.being_deleted) return .missing;
    // Same lock order as the reconcile paths: MultiIndex.lock -> Replicator.mutex.
    if (self.replication) |repl| {
        if (repl.isBootstrapping(name)) return .loading;
    }
    return .ready;
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
    ref.index.checkpoint_age = self.checkpoint_age;
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

const restore_tmp = "data.restore";

/// Bootstrap the (`name`, `generation`) lineage from a donor snapshot streamed via
/// `reader`: restore it into the lineage's data dir and reopen the index in place,
/// returning the new version (the snapshot watermark). The ref — and thus the data
/// consumer — survives; only the underlying Index is swapped, so the consumer resumes
/// from the returned version. Called by the Replicator's consumer after a
/// below-retention read. `transfer_deadline` is the caller's backstop on the
/// transfer; it is disarmed the moment the snapshot is fully drained, so it can
/// never abort the local swap (see disarmTransferDeadline).
pub fn bootstrapLineage(self: *Self, name: []const u8, generation: u64, reader: *std.Io.Reader, transfer_deadline: ?*zio.AutoCancel) !u64 {
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const name_dir = try self.dir.openDir(name, .{ .iterate = true });
    defer name_dir.close();
    const redirect = index_redirect.read(name_dir, self.allocator) catch return error.IndexNotFound;
    defer self.allocator.free(redirect.name);
    if (redirect.deleted or redirect.generation != generation) return error.IndexGenerationMismatch;

    var vbuf: [index_redirect.max_data_dir_len]u8 = undefined;
    const vdir_name = redirect.dataDir(&vbuf);
    const vdir = try name_dir.openDir(vdir_name, .{ .iterate = true });
    defer vdir.close();

    // 1. Restore into a temp data dir (outside the lock — this streams the snapshot).
    deleteDirTree(self.allocator, vdir, restore_tmp) catch {};
    {
        const restore_dir = try openOrCreateDir(vdir, restore_tmp);
        defer restore_dir.close();
        errdefer deleteDirTree(self.allocator, vdir, restore_tmp) catch {};
        try snapshot.restoreInto(restore_dir, reader, a, generation);
    }

    // The snapshot is fully drained; the donor can no longer wedge us. Disarm the
    // transfer backstop before the local swap, which must always run to completion.
    try disarmTransferDeadline(transfer_deadline);

    // 2. Swap it in and reopen the index in place (under the lock, draining borrows).
    return self.installBootstrap(name, generation, name_dir, vdir_name, vdir);
}

const bootstrap_build_tmp = "bootstrap.tmp";

// Disarm a bootstrap transfer deadline once the remote stream is fully drained.
// The deadline exists to bound a wedged transfer; everything after the last byte is
// bounded local work (flush, rename, drain-and-swap) whose interruption costs the
// lineage — installBootstrap's failure path drops it from the map, and the consumer
// then wedges on IndexNotFound. clear() alone is not enough: it only stops a timer
// that has not fired yet, and one that fired in the instant before leaves a
// cancellation already pending on this task, which would otherwise surface at the
// install's first suspension point. checkCancel() surfaces it HERE, where it is
// still harmless, and check() consumes it. A real cancellation (shutdown, delete)
// propagates unchanged — user cancellation has priority in check().
fn disarmTransferDeadline(transfer_deadline: ?*zio.AutoCancel) !void {
    const d = transfer_deadline orelse return;
    d.clear();
    zio.checkCancel() catch |err| {
        if (err == error.Canceled and d.check(error.Canceled)) return;
        return err;
    };
}

/// Bootstrap the (`name`, `generation`) lineage from the feed's own corpus stream
/// (Coordinator.openBootstrap): build a staging index next to the live one, apply
/// every batch at the stream's single position, flush it fully to disk, then swap it
/// in through the same drain-and-reopen path a peer-snapshot restore uses. Returns
/// the position the caller resumes the feed from.
///
/// Staging is what makes a mid-stream death safe. Applying into the live index and
/// dying at 1% would leave a node claiming `position` with 1% of the data, resuming
/// *after* it — and the missing 99% never arrives, because the feed only sends what
/// is above `position`. A dead staging build is just a directory the next attempt
/// deletes. The full flush matters for the same reason: the swap reopens from disk
/// alone and discards the staging WAL, so anything not yet in a file segment would
/// silently vanish from the installed index.
///
/// `transfer_deadline` is the caller's backstop on the stream; it is disarmed the
/// moment the stream is fully drained, so a slow-but-successful transfer can never
/// have its result destroyed by the timer firing into the flush or the swap (see
/// disarmTransferDeadline).
pub fn bootstrapLineageFromSource(self: *Self, name: []const u8, generation: u64, stream: *BootstrapStream, transfer_deadline: ?*zio.AutoCancel) !u64 {
    const name_dir = try self.dir.openDir(name, .{ .iterate = true });
    defer name_dir.close();
    const redirect = index_redirect.read(name_dir, self.allocator) catch return error.IndexNotFound;
    defer self.allocator.free(redirect.name);
    if (redirect.deleted or redirect.generation != generation) return error.IndexGenerationMismatch;

    var vbuf: [index_redirect.max_data_dir_len]u8 = undefined;
    const vdir_name = redirect.dataDir(&vbuf);
    const vdir = try name_dir.openDir(vdir_name, .{ .iterate = true });
    defer vdir.close();

    // Whether anything needs installing is a property of the stream's CONTENT, never
    // of its position. Position 0 with a full corpus is not an edge case, it is the
    // primary migration scenario: the changelog goes live alongside an old corpus
    // and may not have recorded anything yet when the first node arrives. Peek past
    // empty batches before building anything, so the common fresh-lineage case (an
    // empty stream) costs no disk at all.
    const first_batch = blk: {
        while (try stream.next()) |changes| {
            if (changes.len > 0) break :blk changes;
        }
        break :blk null;
    } orelse {
        try disarmTransferDeadline(transfer_deadline); // drained: nothing to install
        return stream.position;
    };

    log.info("bootstrapping '{s}' gen {d} from a source stream at position {d}", .{ name, generation, stream.position });

    // 1. Build the staging index (outside the lock — this is the long part, and the
    //    live index keeps serving searches meanwhile).
    deleteDirTree(self.allocator, vdir, bootstrap_build_tmp) catch {}; // a prior attempt's corpse
    {
        const build_dir = try openOrCreateDir(vdir, bootstrap_build_tmp);
        errdefer deleteDirTree(self.allocator, vdir, bootstrap_build_tmp) catch {};

        // sync=false: the stream's durability is the source's, and an interrupted
        // build starts over either way. No start(): maintenance runs inline per
        // batch, so no background coroutine races the final flush.
        var staging = Index.open(self.allocator, build_dir, self.checkpoint_threshold, false, null) catch |err| {
            build_dir.close();
            return err;
        };
        defer staging.deinit(); // owns (and closes) build_dir

        _ = try staging.update(first_batch, .{ .version = stream.position });
        try staging.runMaintenance();
        while (try stream.next()) |changes| {
            if (changes.len == 0) continue;
            _ = try staging.update(changes, .{ .version = stream.position });
            try staging.runMaintenance();
        }

        // The stream is fully drained; the source can no longer wedge us. Disarm
        // the transfer backstop before the flush and the swap, which must always
        // run to completion once the data has arrived.
        try disarmTransferDeadline(transfer_deadline);

        try staging.flush();
    }

    // 2. Move the staging data dir into the slot the snapshot restore uses, and
    //    reuse its install path (block new borrows, drain, swap, reopen).
    deleteDirTree(self.allocator, vdir, restore_tmp) catch {};
    {
        const build_dir = try vdir.openDir(bootstrap_build_tmp, .{ .iterate = true });
        defer build_dir.close();
        try build_dir.rename("data", vdir, restore_tmp);
    }
    deleteDirTree(self.allocator, vdir, bootstrap_build_tmp) catch {};
    return self.installBootstrap(name, generation, name_dir, vdir_name, vdir);
}

fn installBootstrap(self: *Self, name: []const u8, generation: u64, name_dir: zio.Dir, vdir_name: []const u8, vdir: zio.Dir) !u64 {
    try self.lock.lock();
    defer self.lock.unlock();

    const ref = self.indexes.get(name) orelse return error.IndexNotFound;
    if (ref.being_deleted or ref.generation != generation) return error.IndexGenerationMismatch;

    // Block new borrows, drain outstanding ones (searches) — same as dropIndex.
    ref.being_deleted = true;
    while (ref.references > 1) {
        ref.released.wait(&self.lock) catch |err| {
            ref.being_deleted = false;
            ref.released.broadcast();
            return err;
        };
    }

    // Close the live index, swap data <- data.restore, drop the stale WAL, reopen.
    ref.index.deinit();
    self.swapAndReopen(ref, name_dir, vdir_name, vdir) catch |err| {
        // The Index is now deinit'd and unusable; remove it so nothing touches it and
        // let the meta consumer rebuild the lineage.
        const kv = self.indexes.fetchRemove(name).?;
        self.allocator.free(kv.key);
        self.allocator.destroy(kv.value);
        return err;
    };
    ref.being_deleted = false;
    ref.released.broadcast();
    return ref.index.version;
}

fn swapAndReopen(self: *Self, ref: *IndexRef, name_dir: zio.Dir, vdir_name: []const u8, vdir: zio.Dir) !void {
    deleteDirTree(self.allocator, vdir, "data") catch {};
    try vdir.rename(restore_tmp, vdir, "data");
    deleteDirTree(self.allocator, vdir, "oplog") catch {}; // Index.open recreates it empty

    const data_dir = try name_dir.openDir(vdir_name, .{ .iterate = true });
    ref.index = Index.open(self.allocator, data_dir, self.checkpoint_threshold, self.sync, null) catch |err| {
        data_dir.close();
        return err;
    };
    ref.index.checkpoint_age = self.checkpoint_age;
    try ref.index.start();
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
