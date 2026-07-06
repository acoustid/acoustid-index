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
const Replicator = @import("Replicator.zig");
const Coordinator = @import("Coordinator.zig").Coordinator;
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
    references: usize = 1,
    being_deleted: bool = false,
    released: zio.Condition = .init,
};

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
            if (err != error.IndexNotOpened) fatal = fatal orelse err;
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
    const index_dir = try self.dir.openDir(name, .{ .iterate = true });
    const ref = try self.allocator.create(IndexRef);
    errdefer self.allocator.destroy(ref);
    ref.* = .{ .index = undefined };
    ref.index = Index.open(self.allocator, index_dir, self.checkpoint_threshold, self.sync, load_sem) catch |err| {
        index_dir.close();
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
    _ = arena;
    // Replicated mode: the write goes to the log; the consumer applies it.
    if (self.replication) |repl| return repl.update(name, request);

    const index = try self.getIndex(name);
    defer self.releaseIndex(index);
    metrics.incUpdates();
    const version = try index.update(request.changes, request.metadata, .{ .expected_version = request.expected_version });
    return .{ .version = version };
}

/// Apply changes at an externally-assigned version (the replicated consumer's
/// apply path; version = changelog id). The external log owns ordering and
/// durability, so this just stamps the version onto the local oplog + segments.
pub fn applyLog(self: *Self, name: []const u8, changes: []const Change, metadata: ?Metadata, version: u64) !void {
    const index = try self.getIndex(name);
    defer self.releaseIndex(index);
    metrics.incUpdates();
    _ = try index.update(changes, metadata, .{ .version = version });
}

/// The index's current version (max applied changelog id) — where a consumer
/// resumes after local oplog replay.
pub fn indexVersion(self: *Self, name: []const u8) !u64 {
    const index = try self.getIndex(name);
    defer self.releaseIndex(index);
    return index.version;
}

/// Snapshot of the current index names (owned: caller frees each string and the
/// slice). Used to launch a consumer per existing index at startup.
pub fn listIndexNames(self: *Self, allocator: std.mem.Allocator) ![][]const u8 {
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

pub fn createIndex(self: *Self, name: []const u8, request: api.CreateIndexRequest) !api.CreateIndexResponse {
    if (!isValidName(name)) return error.InvalidIndexName;

    try self.lock.lock();
    defer self.lock.unlock();

    if (self.indexes.get(name)) |existing| {
        if (!existing.being_deleted) {
            if (request.expect_does_not_exist) return error.IndexAlreadyExists;
            return .{ .version = existing.index.version, .ready = true, .generation = request.generation orelse 0 };
        }
        // being deleted: fall through and let the delete finish would be racy;
        // for now treat a concurrently-deleting index as already existing.
        return error.IndexAlreadyExists;
    }

    self.dir.createDir(name, 0o755) catch |err| switch (err) {
        error.PathAlreadyExists => {}, // reuse an orphaned dir from a prior run
        else => return err,
    };

    const index_dir = try self.dir.openDir(name, .{ .iterate = true });
    const ref = try self.allocator.create(IndexRef);
    errdefer self.allocator.destroy(ref);
    ref.* = .{ .index = undefined };
    ref.index = Index.open(self.allocator, index_dir, self.checkpoint_threshold, self.sync, null) catch |err| {
        index_dir.close();
        return err;
    };
    errdefer ref.index.deinit();
    try ref.index.start();

    const name_copy = try self.allocator.dupe(u8, name);
    errdefer self.allocator.free(name_copy);

    try self.indexes.put(self.allocator, name_copy, ref);

    // In replicated mode, start consuming the log for this index. Only touches
    // the Replicator lock + spawns, so it's safe under the manager lock.
    if (self.replication) |repl| {
        repl.addConsumer(name, ref.index.version) catch |err| {
            log.warn("failed to start replication consumer for '{s}': {}", .{ name, err });
        };
    }
    return .{ .version = ref.index.version, .ready = true, .generation = request.generation orelse 0 };
}

pub fn deleteIndex(self: *Self, name: []const u8, request: api.DeleteIndexRequest) !api.DeleteIndexResponse {
    // Stop the consumer first (before holding the manager lock) so its in-flight
    // apply can finish and its borrow drains for the wait below.
    if (self.replication) |repl| repl.removeConsumer(name);

    try self.lock.lock();
    defer self.lock.unlock();

    const ref = self.indexes.get(name) orelse {
        if (request.expect_exists) return error.IndexNotFound;
        return .{ .deleted = false };
    };
    if (ref.being_deleted) {
        if (request.expect_exists) return error.IndexNotFound;
        return .{ .deleted = false };
    }

    // Block new borrows, then wait for outstanding ones to drain to the map's
    // own reference. releaseIndex broadcasts on each drop; wait releases the lock.
    ref.being_deleted = true;
    while (ref.references > 1) {
        ref.released.wait(&self.lock) catch |err| {
            ref.being_deleted = false; // a cancelled delete must not disable the index
            ref.released.broadcast();
            return err;
        };
    }

    const kv = self.indexes.fetchRemove(name).?;
    kv.value.index.deinit();
    self.allocator.destroy(kv.value);
    self.allocator.free(kv.key);
    self.removeIndexDir(name) catch |err| {
        log.warn("failed to remove index dir '{s}': {}", .{ name, err });
    };
    return .{ .deleted = true };
}

// Delete the whole index dir (the oplog/ and data/ subdirs and their contents).
fn removeIndexDir(self: *Self, name: []const u8) !void {
    try @import("common.zig").deleteDirTree(self.allocator, self.dir, name);
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
