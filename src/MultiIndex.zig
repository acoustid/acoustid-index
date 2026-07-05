// Manages a set of named in-memory indexes and exposes the high-level
// operations the HTTP handlers call. Request/response shapes come from api.zig.
//
// The map lock is held (shared) for the duration of search/update so a
// concurrent delete can't free the index mid-operation. A later IndexRef
// refcount can replace this without changing the method signatures.

const std = @import("std");
const zio = @import("zio");
const api = @import("api.zig");
const Index = @import("Index.zig");
const SearchResults = @import("common.zig").SearchResults;

const Self = @This();

allocator: std.mem.Allocator,
lock: zio.RwLock = .init,
indexes: std.StringHashMapUnmanaged(*Index) = .empty,

pub fn init(allocator: std.mem.Allocator) Self {
    return .{ .allocator = allocator };
}

pub fn deinit(self: *Self) void {
    var it = self.indexes.iterator();
    while (it.next()) |entry| {
        entry.value_ptr.*.deinit();
        self.allocator.destroy(entry.value_ptr.*);
        self.allocator.free(entry.key_ptr.*);
    }
    self.indexes.deinit(self.allocator);
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

    try index.search(request.query, &collector);

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
    const version = try index.update(request.changes, request.metadata, .{ .expected_version = request.expected_version });
    return .{ .version = version };
}

pub fn checkIndexExists(self: *Self, name: []const u8) !bool {
    try self.lock.lockShared();
    defer self.lock.unlockShared();
    return self.indexes.contains(name);
}

pub fn createIndex(self: *Self, name: []const u8, request: api.CreateIndexRequest) !api.CreateIndexResponse {
    try self.lock.lock();
    defer self.lock.unlock();

    if (self.indexes.get(name)) |existing| {
        if (request.expect_does_not_exist) return error.IndexAlreadyExists;
        return .{ .version = existing.version, .ready = true, .generation = request.generation orelse 0 };
    }

    const index = try self.allocator.create(Index);
    errdefer self.allocator.destroy(index);
    index.* = Index.init(self.allocator);

    const name_copy = try self.allocator.dupe(u8, name);
    errdefer self.allocator.free(name_copy);

    try self.indexes.put(self.allocator, name_copy, index);
    return .{ .version = 0, .ready = true, .generation = request.generation orelse 0 };
}

pub fn deleteIndex(self: *Self, name: []const u8, request: api.DeleteIndexRequest) !api.DeleteIndexResponse {
    try self.lock.lock();
    defer self.lock.unlock();

    if (self.indexes.fetchRemove(name)) |kv| {
        kv.value.deinit();
        self.allocator.destroy(kv.value);
        self.allocator.free(kv.key);
        return .{ .deleted = true };
    }
    if (request.expect_exists) return error.IndexNotFound;
    return .{ .deleted = false };
}

pub fn getIndexInfo(self: *Self, arena: std.mem.Allocator, name: []const u8) !api.GetIndexInfoResponse {
    _ = self;
    _ = arena;
    _ = name;
    // TODO: needs Metadata JSON serialization; wire with the info endpoints.
    return error.NotImplemented;
}

pub fn getFingerprintInfo(self: *Self, arena: std.mem.Allocator, name: []const u8, id: u32) !api.GetFingerprintInfoResponse {
    _ = self;
    _ = arena;
    _ = name;
    _ = id;
    return error.NotImplemented;
}

pub fn checkFingerprintExists(self: *Self, name: []const u8, id: u32) !bool {
    _ = self;
    _ = name;
    _ = id;
    return error.NotImplemented;
}
