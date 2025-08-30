const std = @import("std");
const log = std.log.scoped(.multi_index);
const assert = std.debug.assert;

const Index = @import("Index.zig");
const Scheduler = @import("utils/Scheduler.zig");
const api = @import("api.zig");
const SearchResults = @import("common.zig").SearchResults;
const Deadline = @import("utils/Deadline.zig");
const metrics = @import("metrics.zig");

const Self = @This();

const DELETE_TIMEOUT_MS = 5000; // 5 seconds timeout for deletion

pub const IndexRef = struct {
    index: Index,
    references: usize = 0,
    delete_files: bool = false,
    being_deleted: bool = false,

    pub fn incRef(self: *IndexRef) void {
        self.references += 1;
    }

    pub fn decRef(self: *IndexRef) bool {
        assert(self.references > 0);
        self.references -= 1;
        return self.references == 0;
    }
};

lock: std.Thread.Mutex = .{},
lock_file: ?std.fs.File = null,
allocator: std.mem.Allocator,
scheduler: *Scheduler,
dir: std.fs.Dir,
index_options: Index.Options,
indexes: std.StringHashMapUnmanaged(IndexRef) = .{},

fn isValidName(name: []const u8) bool {
    for (name, 0..) |c, i| {
        if (i == 0) {
            switch (c) {
                '0'...'9', 'A'...'Z', 'a'...'z' => {},
                else => return false,
            }
        } else {
            switch (c) {
                '0'...'9', 'A'...'Z', 'a'...'z', '_', '-' => {},
                else => return false,
            }
        }
    }
    return true;
}

test "isValidName" {
    try std.testing.expect(isValidName("a"));
    try std.testing.expect(isValidName("a1"));
    try std.testing.expect(isValidName("a1-b"));
    try std.testing.expect(isValidName("a1_b"));
    try std.testing.expect(!isValidName("_1b2"));
    try std.testing.expect(!isValidName("-1b2"));
    try std.testing.expect(!isValidName("a/a"));
    try std.testing.expect(!isValidName(".foo"));
}

pub fn init(allocator: std.mem.Allocator, scheduler: *Scheduler, dir: std.fs.Dir, index_options: Index.Options) Self {
    return .{
        .allocator = allocator,
        .scheduler = scheduler,
        .dir = dir,
        .index_options = index_options,
    };
}

fn openIndex(self: *Self, path: []const u8, comptime create: bool) !*IndexRef {
    log.info("loading index {s}", .{path});

    const name = try self.allocator.dupe(u8, path);
    errdefer self.allocator.free(name);

    var result = try self.indexes.getOrPut(self.allocator, name);
    if (result.found_existing) {
        unreachable;
    }
    errdefer self.indexes.removeByPtr(result.key_ptr);

    result.value_ptr.* = .{
        .index = try Index.init(self.allocator, self.scheduler, self.dir, name, self.index_options),
        .references = 1,
    };
    errdefer result.value_ptr.index.deinit();

    result.value_ptr.index.open(create) catch |err| {
        if (create) {
            log.err("failed to create index {s}: {}", .{ name, err });
        } else {
            log.err("failed to open index {s}: {}", .{ name, err });
        }
        return err;
    };

    return result.value_ptr;
}

pub fn open(self: *Self) !void {
    self.lock.lock();
    defer self.lock.unlock();

    var lock_file = self.dir.createFile(".lock", .{}) catch |err| {
        log.err("failed to open/create lock file: {}", .{err});
        return err;
    };
    errdefer lock_file.close();

    lock_file.lock(.exclusive) catch |err| {
        log.err("failed to acquire lock file: {}", .{err});
        return err;
    };
    errdefer lock_file.unlock();

    var iter = self.dir.iterate();
    while (try iter.next()) |entry| {
        if (entry.kind != .directory) {
            continue;
        }
        if (!isValidName(entry.name)) {
            log.warn("skipping unexpected directory {s}", .{entry.name});
            continue;
        }
        _ = try self.openIndex(entry.name, false);
    }

    self.lock_file = lock_file;
}

pub fn deinit(self: *Self) void {
    self.lock.lock();
    defer self.lock.unlock();

    if (self.lock_file) |file| {
        file.unlock();
        file.close();
    }

    var iter = self.indexes.iterator();
    while (iter.next()) |entry| {
        entry.value_ptr.index.deinit();
        self.allocator.free(entry.key_ptr.*);
    }
    self.indexes.deinit(self.allocator);
}

fn deleteIndexFiles(self: *Self, name: []const u8) !void {
    const tmp_name = try std.mem.concat(self.allocator, u8, &[_][]const u8{ name, ".delete" });
    defer self.allocator.free(tmp_name);

    self.dir.rename(name, tmp_name) catch |err| {
        if (err == error.FileNotFound) {
            return;
        }
        log.err("failed to rename index directory {s} to {s}: {}", .{ name, tmp_name, err });
        return err;
    };

    self.dir.deleteTree(tmp_name) catch |err| {
        log.err("failed to delete index directory {s}: {}", .{ tmp_name, err });
    };
}

pub fn releaseIndex(self: *Self, index: *Index) void {
    self.lock.lock();
    defer self.lock.unlock();

    const index_ref: *IndexRef = @fieldParentPtr("index", index);
    const can_delete = index_ref.decRef();
    // the last ref should be held by the internal map and that's only released in deleteIndex
    std.debug.assert(!can_delete);
}

fn borrowIndex(index_ref: *IndexRef) !*Index {
    index_ref.incRef();
    return &index_ref.index;
}

pub fn getOrCreateIndex(self: *Self, name: []const u8, create: bool) !*Index {
    if (!isValidName(name)) {
        return error.InvalidIndexName;
    }

    self.lock.lock();
    defer self.lock.unlock();

    if (self.indexes.getEntry(name)) |entry| {
        if (entry.value_ptr.being_deleted) {
            return error.IndexBeingDeleted;
        }
        return borrowIndex(entry.value_ptr);
    }

    if (!create) {
        return error.IndexNotFound;
    }

    log.info("creating index {s}", .{name});

    const index_ref = try self.openIndex(name, true);
    return borrowIndex(index_ref);
}

pub fn getIndex(self: *Self, name: []const u8) !*Index {
    return self.getOrCreateIndex(name, false);
}

pub fn deleteIndex(self: *Self, name: []const u8) !void {
    if (!isValidName(name)) {
        return error.InvalidIndexName;
    }

    self.lock.lock();
    defer self.lock.unlock();

    const entry = self.indexes.getEntry(name) orelse return;

    // Mark as being deleted to prevent new references
    if (entry.value_ptr.being_deleted) {
        return error.IndexAlreadyBeingDeleted;
    }
    entry.value_ptr.being_deleted = true;
    defer {
        // Reset flag if we fail to delete
        if (entry.value_ptr.being_deleted) {
            entry.value_ptr.being_deleted = false;
        }
    }

    log.info("marking index {s} for deletion, waiting for references to be released", .{name});

    // Wait for all references except the map's reference to be released
    const start_time = std.time.milliTimestamp();
    while (entry.value_ptr.references > 1) {
        const current_time = std.time.milliTimestamp();
        if (current_time - start_time > DELETE_TIMEOUT_MS) {
            log.warn("timeout waiting for index {s} references to be released (current: {d})", .{ name, entry.value_ptr.references });
            return error.DeleteTimeout;
        }

        // Release the lock briefly to allow other operations
        self.lock.unlock();
        std.time.sleep(10 * std.time.ns_per_ms); // Sleep for 10ms
        self.lock.lock();

        // Check if the entry still exists (in case of concurrent operations)
        const current_entry = self.indexes.getEntry(name) orelse return error.IndexNotFound;
        if (current_entry.value_ptr != entry.value_ptr) {
            return error.IndexNotFound;
        }
    }

    // At this point, only the map holds a reference
    const can_delete = entry.value_ptr.decRef();
    assert(can_delete); // Should always be true since we waited for references == 1

    log.info("deleting index {s}", .{name});

    self.deleteIndexFiles(name) catch |err| {
        entry.value_ptr.incRef();
        return err;
    };

    // Clear the being_deleted flag since we're about to remove the entry
    entry.value_ptr.being_deleted = false;

    // Ensure Index can safely log/use the name during deinit
    const key_mem = entry.key_ptr.*;
    entry.value_ptr.index.deinit();
    self.indexes.removeByPtr(entry.key_ptr);
    self.allocator.free(key_mem);
}

pub fn search(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
    request: api.SearchRequest,
) !api.SearchResponse {
    const index = try self.getIndex(index_name);
    defer self.releaseIndex(index);

    // Validate and clamp limits
    const limit = @max(@min(request.limit, api.max_search_limit), api.min_search_limit);
    const timeout = @min(request.timeout, api.max_search_timeout);
    const deadline = Deadline.init(timeout);

    metrics.search();

    var collector = SearchResults.init(allocator, .{
        .max_results = limit,
        .min_score = @intCast((request.query.len + 19) / 20),
        .min_score_pct = 10,
    });
    defer collector.deinit();

    try index.search(request.query, &collector, deadline);

    const results = collector.getResults();

    if (results.len == 0) {
        metrics.searchMiss();
    } else {
        metrics.searchHit();
    }

    // Convert results to API format
    const response_results = try allocator.alloc(api.SearchResult, results.len);
    for (results, 0..) |r, i| {
        response_results[i] = .{ .id = r.id, .score = r.score };
    }

    return api.SearchResponse{ .results = response_results };
}

pub fn update(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
    request: api.UpdateRequest,
) !api.UpdateResponse {
    _ = allocator; // Response doesn't need allocation

    const index = try self.getIndex(index_name);
    defer self.releaseIndex(index);

    metrics.update(request.changes.len);

    const new_version = try index.update(
        request.changes,
        request.metadata,
        request.expected_version,
    );

    return api.UpdateResponse{ .version = new_version };
}

pub fn getIndexInfo(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
) !api.GetIndexInfoResponse {
    const index = try self.getIndex(index_name);
    defer self.releaseIndex(index);

    var index_reader = try index.acquireReader();
    defer index.releaseReader(&index_reader);

    return api.GetIndexInfoResponse{
        .version = index_reader.getVersion(),
        .metadata = try index_reader.getMetadata(allocator),
        .stats = api.IndexStats{
            .min_doc_id = index_reader.getMinDocId(),
            .max_doc_id = index_reader.getMaxDocId(),
            .num_segments = index_reader.getNumSegments(),
            .num_docs = index_reader.getNumDocs(),
        },
    };
}

pub fn checkIndexExists(
    self: *Self,
    index_name: []const u8,
) !void {
    const index = try self.getIndex(index_name);
    defer self.releaseIndex(index);
    // Just checking existence, no need to return anything
}

pub fn createIndex(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
) !api.CreateIndexResponse {
    _ = allocator; // Response doesn't need allocation

    const index = try self.getOrCreateIndex(index_name, true);
    defer self.releaseIndex(index);

    var index_reader = try index.acquireReader();
    defer index.releaseReader(&index_reader);

    return api.CreateIndexResponse{
        .version = index_reader.getVersion(),
    };
}

pub fn getFingerprintInfo(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
    fingerprint_id: u32,
) !api.GetFingerprintInfoResponse {
    _ = allocator; // Response doesn't need allocation

    const index = try self.getIndex(index_name);
    defer self.releaseIndex(index);

    var index_reader = try index.acquireReader();
    defer index.releaseReader(&index_reader);

    const info = try index_reader.getDocInfo(fingerprint_id) orelse {
        return error.FingerprintNotFound;
    };

    return api.GetFingerprintInfoResponse{
        .version = info.version,
    };
}

pub fn checkFingerprintExists(
    self: *Self,
    index_name: []const u8,
    fingerprint_id: u32,
) !void {
    const index = try self.getIndex(index_name);
    defer self.releaseIndex(index);

    var index_reader = try index.acquireReader();
    defer index.releaseReader(&index_reader);

    const info = try index_reader.getDocInfo(fingerprint_id);
    if (info == null) {
        return error.FingerprintNotFound;
    }
}
