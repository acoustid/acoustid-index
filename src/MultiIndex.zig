// Manages a set of named indexes, each stored in its own subdirectory of the
// data root (the directory's presence is the index's existence). Exposes the
// high-level operations the HTTP handlers call.
//
// The map lock is held (shared) for the duration of search/update so a
// concurrent delete can't free an index mid-operation; create/delete take it
// exclusive. Each Index has its own lock, so updates to different indexes run
// concurrently.

const std = @import("std");
const zio = @import("zio");
const api = @import("api.zig");
const Index = @import("Index.zig");
const SearchResults = @import("common.zig").SearchResults;
const metrics = @import("metrics.zig");
const log = std.log.scoped(.multi_index);

const Self = @This();

allocator: std.mem.Allocator,
dir: zio.Dir,
lock: zio.RwLock = .init,
indexes: std.StringHashMapUnmanaged(*Index) = .empty,
checkpoint_threshold: usize = 100_000,
// Whether index oplogs fsync each append (false when an upstream owns durability).
sync: bool = true,
// Max file-segment loads in flight across all indexes during open(); 0 = no limit.
load_concurrency: usize = 0,

pub fn init(allocator: std.mem.Allocator, dir: zio.Dir) Self {
    return .{ .allocator = allocator, .dir = dir };
}

pub fn deinit(self: *Self) void {
    var it = self.indexes.iterator();
    while (it.next()) |entry| {
        entry.value_ptr.*.deinit();
        self.allocator.destroy(entry.value_ptr.*);
        self.allocator.free(entry.key_ptr.*);
    }
    self.indexes.deinit(self.allocator);
    self.dir.close();
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
    const indexes = try self.allocator.alloc(*Index, names.items.len);
    defer self.allocator.free(indexes);
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
            group.spawn(openOneIndex, .{ self, name, sem_ptr, &indexes[i], &results[i] }) catch |err| {
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
            const index = indexes[i];
            self.indexes.put(self.allocator, name, index) catch |err| {
                index.deinit();
                self.allocator.destroy(index);
                self.allocator.free(name);
                fatal = fatal orelse err;
                continue;
            };
            log.info("opened index '{s}' at version {d}", .{ name, index.version });
        } else |err| {
            self.allocator.free(name);
            if (err != error.IndexNotOpened) fatal = fatal orelse err;
        }
    }
    if (fatal) |err| return err;
}

// Group-spawned: opens one index into out_index, capturing its result into
// out_res (void return keeps the group's error flags clean).
fn openOneIndex(self: *Self, name: []const u8, load_sem: ?*zio.Semaphore, out_index: **Index, out_res: *anyerror!void) void {
    out_res.* = self.openOneIndexInner(name, load_sem, out_index);
}

fn openOneIndexInner(self: *Self, name: []const u8, load_sem: ?*zio.Semaphore, out_index: **Index) !void {
    const index_dir = try self.dir.openDir(name, .{ .iterate = true });
    const index = try self.allocator.create(Index);
    errdefer self.allocator.destroy(index);
    index.* = Index.open(self.allocator, index_dir, self.checkpoint_threshold, self.sync, load_sem) catch |err| {
        index_dir.close();
        return err;
    };
    errdefer index.deinit();
    try index.start();
    out_index.* = index;
}

/// Search an index. Options ride in the request (limit, min_score, score_pct,
/// timeout); callers sanitize untrusted values first. `timeout == 0` = no bound.
pub fn search(self: *Self, arena: std.mem.Allocator, name: []const u8, request: api.SearchRequest) !api.SearchResponse {
    try self.lock.lockShared();
    defer self.lock.unlockShared();

    const index = self.indexes.get(name) orelse return error.IndexNotFound;
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
    try self.lock.lockShared();
    defer self.lock.unlockShared();

    const index = self.indexes.get(name) orelse return error.IndexNotFound;
    metrics.incUpdates();
    const version = try index.update(request.changes, request.metadata, .{ .expected_version = request.expected_version });
    return .{ .version = version };
}

/// Render metrics (global counters + a per-index docs gauge) in Prometheus text.
pub fn writeMetrics(self: *Self, w: *std.Io.Writer) !void {
    try metrics.writeGlobal(w);

    try self.lock.lockShared();
    defer self.lock.unlockShared();

    try w.writeAll("# HELP fpindex_docs Number of documents in an index\n# TYPE fpindex_docs gauge\n");
    var it = self.indexes.iterator();
    while (it.next()) |entry| {
        var reader = try entry.value_ptr.*.acquireReader();
        defer reader.deinit();
        try w.print("fpindex_docs{{index=\"{s}\"}} {d}\n", .{ entry.key_ptr.*, reader.numDocs() });
    }
}

pub fn checkIndexExists(self: *Self, name: []const u8) !bool {
    try self.lock.lockShared();
    defer self.lock.unlockShared();
    return self.indexes.contains(name);
}

pub fn createIndex(self: *Self, name: []const u8, request: api.CreateIndexRequest) !api.CreateIndexResponse {
    if (!isValidName(name)) return error.InvalidIndexName;

    try self.lock.lock();
    defer self.lock.unlock();

    if (self.indexes.get(name)) |existing| {
        if (request.expect_does_not_exist) return error.IndexAlreadyExists;
        return .{ .version = existing.version, .ready = true, .generation = request.generation orelse 0 };
    }

    self.dir.createDir(name, 0o755) catch |err| switch (err) {
        error.PathAlreadyExists => {}, // reuse an orphaned dir from a prior run
        else => return err,
    };

    const index_dir = try self.dir.openDir(name, .{ .iterate = true });
    const index = try self.allocator.create(Index);
    errdefer self.allocator.destroy(index);
    index.* = Index.open(self.allocator, index_dir, self.checkpoint_threshold, self.sync, null) catch |err| {
        index_dir.close();
        return err;
    };
    errdefer index.deinit();
    try index.start();

    const name_copy = try self.allocator.dupe(u8, name);
    errdefer self.allocator.free(name_copy);

    try self.indexes.put(self.allocator, name_copy, index);
    return .{ .version = index.version, .ready = true, .generation = request.generation orelse 0 };
}

pub fn deleteIndex(self: *Self, name: []const u8, request: api.DeleteIndexRequest) !api.DeleteIndexResponse {
    try self.lock.lock();
    defer self.lock.unlock();

    if (self.indexes.fetchRemove(name)) |kv| {
        kv.value.deinit();
        self.allocator.destroy(kv.value);
        self.allocator.free(kv.key);
        self.removeIndexDir(name) catch |err| {
            log.warn("failed to remove index dir '{s}': {}", .{ name, err });
        };
        return .{ .deleted = true };
    }
    if (request.expect_exists) return error.IndexNotFound;
    return .{ .deleted = false };
}

// Delete the whole index dir (the oplog/ and data/ subdirs and their contents).
fn removeIndexDir(self: *Self, name: []const u8) !void {
    try @import("common.zig").deleteDirTree(self.allocator, self.dir, name);
}

pub fn getIndexInfo(self: *Self, arena: std.mem.Allocator, name: []const u8) !api.GetIndexInfoResponse {
    try self.lock.lockShared();
    defer self.lock.unlockShared();

    const index = self.indexes.get(name) orelse return error.IndexNotFound;
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
    try self.lock.lockShared();
    defer self.lock.unlockShared();

    const index = self.indexes.get(name) orelse return error.IndexNotFound;
    var reader = try index.acquireReader();
    defer reader.deinit();

    const info = reader.getDocInfo(id) orelse return error.FingerprintNotFound;
    if (info.deleted) return error.FingerprintNotFound;
    return .{ .version = info.version };
}

pub fn checkFingerprintExists(self: *Self, name: []const u8, id: u32) !bool {
    try self.lock.lockShared();
    defer self.lock.unlockShared();

    const index = self.indexes.get(name) orelse return error.IndexNotFound;
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
