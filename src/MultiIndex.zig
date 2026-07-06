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
const log = std.log.scoped(.multi_index);

const Self = @This();

allocator: std.mem.Allocator,
dir: zio.Dir,
lock: zio.RwLock = .init,
indexes: std.StringHashMapUnmanaged(*Index) = .empty,
checkpoint_threshold: usize = 100_000,

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

/// Discover existing indexes (subdirectories of the data root) and replay each
/// one's oplog to rebuild its in-memory state.
pub fn open(self: *Self) !void {
    var it = self.dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind != .directory) continue;
        if (!isValidName(entry.name)) {
            log.warn("skipping unexpected entry '{s}' in data dir", .{entry.name});
            continue;
        }
        // entry.name is only valid until the next it.next(); dupe before any I/O.
        const name = try self.allocator.dupe(u8, entry.name);
        errdefer self.allocator.free(name);

        const index_dir = try self.dir.openDir(name, .{ .iterate = true });
        const index = try self.allocator.create(Index);
        errdefer self.allocator.destroy(index);
        index.* = Index.open(self.allocator, index_dir, self.checkpoint_threshold) catch |err| {
            index_dir.close();
            return err;
        };
        errdefer index.deinit();
        try index.start();

        try self.indexes.put(self.allocator, name, index);
        log.info("opened index '{s}' at version {d}", .{ name, index.version });
    }
}

pub fn search(self: *Self, arena: std.mem.Allocator, name: []const u8, request: api.SearchRequest) !api.SearchResponse {
    try self.lock.lockShared();
    defer self.lock.unlockShared();

    const index = self.indexes.get(name) orelse return error.IndexNotFound;

    const limit = @max(@min(request.limit, api.max_search_limit), api.min_search_limit);
    var collector = SearchResults.init(arena, .{
        .max_results = limit,
        .min_score = @intCast((request.query.len + 19) / 20),
        .min_score_pct = 10,
    });
    // arena-backed; freed with the request arena.

    var reader = try index.acquireReader();
    defer reader.deinit();

    // Bound the search: the segment scans hit maybeYield, so an expired timer
    // cancels the task there. check() tells our timeout apart from a real
    // (shutdown) cancellation, which still propagates.
    const timeout_ms = if (request.timeout == 0) api.default_search_timeout else @min(request.timeout, api.max_search_timeout);
    var deadline: zio.AutoCancel = .init;
    deadline.set(.fromMilliseconds(timeout_ms));
    defer deadline.clear();

    reader.search(request.query, &collector) catch |err| {
        if (err == error.Canceled and deadline.check(error.Canceled)) return error.SearchTimeout;
        return err;
    };

    const results = collector.getResults();
    const out = try arena.alloc(api.SearchResult, results.len);
    for (results, 0..) |r, i| out[i] = .{ .id = r.id, .score = r.score };
    return .{ .results = out };
}

pub fn update(self: *Self, arena: std.mem.Allocator, name: []const u8, request: api.UpdateRequest) !api.UpdateResponse {
    _ = arena;
    try self.lock.lockShared();
    defer self.lock.unlockShared();

    const index = self.indexes.get(name) orelse return error.IndexNotFound;
    const version = try index.update(request.changes, request.metadata, request.expected_version);
    return .{ .version = version };
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
    index.* = Index.open(self.allocator, index_dir, self.checkpoint_threshold) catch |err| {
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
