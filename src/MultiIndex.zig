const std = @import("std");
const log = std.log.scoped(.multi_index);
const assert = std.debug.assert;

const Index = @import("Index.zig");
const Scheduler = @import("utils/Scheduler.zig");
const api = @import("api.zig");
const SearchResults = @import("common.zig").SearchResults;
const Deadline = @import("utils/Deadline.zig");
const metrics = @import("metrics.zig");
const index_redirect = @import("index_redirect.zig");
const IndexRedirect = index_redirect.IndexRedirect;

const Self = @This();

const DELETE_TIMEOUT_MS = 5000; // 5 seconds timeout for deletion

const OptionalIndex = struct {
    value: Index = undefined,
    has_value: bool = false,

    pub fn clear(self: *OptionalIndex) void {
        if (self.has_value) {
            self.value.deinit();
        }
        self.has_value = false;
    }

    pub fn get(self: *OptionalIndex) ?*Index {
        if (self.has_value) {
            return &self.value;
        }
        return null;
    }
};

pub const IndexRef = struct {
    index: OptionalIndex = .{},
    index_dir: std.fs.Dir,
    redirect: IndexRedirect,
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

    pub fn deinit(self: *IndexRef) void {
        self.index.clear();
        self.index_dir.close();
    }
};

lock: std.Thread.Mutex = .{},
lock_file: ?std.fs.File = null,
allocator: std.mem.Allocator,
scheduler: *Scheduler,
dir: std.fs.Dir,
index_options: Index.Options,
indexes: std.StringHashMapUnmanaged(IndexRef) = .{},
cleanup_task: ?Scheduler.Task = null,

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

fn openIndex(self: *Self, path: []const u8, create: bool) !*IndexRef {
    log.info("loading index {s}", .{path});

    const name = try self.allocator.dupe(u8, path);
    errdefer self.allocator.free(name);

    var save_redirect = false;

    var result = try self.indexes.getOrPut(self.allocator, name);
    errdefer if (!result.found_existing) {
        self.indexes.removeByPtr(result.key_ptr);
    };

    // Set defaults
    if (!result.found_existing) {
        result.value_ptr.index = .{ .has_value = false };
        result.value_ptr.references = 1;
    } else {
        assert(!result.value_ptr.index.has_value);
    }

    // Open index directory, if needed
    if (!result.found_existing) {
        result.value_ptr.index_dir = try self.dir.makeOpenPath(name, .{});
    }
    errdefer if (!result.found_existing) {
        result.value_ptr.index_dir.close();
    };

    // Load redirect file, if needed
    if (!result.found_existing) {
        result.value_ptr.redirect = index_redirect.readRedirectFile(result.value_ptr.index_dir, self.allocator) catch |err| blk: {
            if (err == error.FileNotFound and create) {
                break :blk IndexRedirect.init(name);
            } else {
                return err;
            }
        };
    } else {
        assert(result.value_ptr.redirect.deleted == true);
    }

    // Increment version, in case of a previously deleted index
    var previous_redirect: ?IndexRedirect = null;
    if (result.value_ptr.redirect.deleted) {
        if (!create) {
            return error.IndexNotFound;
        }
        previous_redirect = result.value_ptr.redirect;
        result.value_ptr.redirect = result.value_ptr.redirect.nextVersion();
        save_redirect = true;
    }
    errdefer if (previous_redirect) |prev| {
        result.value_ptr.redirect = prev;
    };

    // Generate data directory name
    const data_path = try result.value_ptr.redirect.getDataDir(self.allocator);
    errdefer self.allocator.free(data_path);

    result.value_ptr.index.value = try Index.init(self.allocator, self.scheduler, result.value_ptr.index_dir, name, data_path, self.index_options);
    result.value_ptr.index.has_value = true;
    errdefer result.value_ptr.deinit();

    result.value_ptr.index.value.open(create) catch |err| {
        if (create) {
            log.err("failed to create index {s}: {}", .{ name, err });
        } else {
            log.err("failed to open index {s}: {}", .{ name, err });
        }
        return err;
    };

    // If this was a create operation, write the redirect file
    if (save_redirect) {
        try index_redirect.writeRedirectFile(result.value_ptr.index_dir, result.value_ptr.redirect);
    }

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

        _ = self.openIndex(entry.name, false) catch |err| {
            log.warn("failed to open index {s}: {}", .{ entry.name, err });
        };
    }

    self.lock_file = lock_file;

    // Schedule periodic cleanup task
    self.cleanup_task = try self.scheduler.createTask(.low, cleanupDeletedIndexesTask, .{self});
    if (self.cleanup_task) |task| {
        self.scheduler.scheduleTask(task); // Start cleanup immediately
    }
}

pub fn deinit(self: *Self) void {
    self.lock.lock();
    defer self.lock.unlock();

    if (self.cleanup_task) |task| {
        self.scheduler.destroyTask(task);
    }

    if (self.lock_file) |file| {
        file.unlock();
        file.close();
    }

    var iter = self.indexes.iterator();
    while (iter.next()) |entry| {
        entry.value_ptr.deinit();
        self.allocator.free(entry.key_ptr.*);
    }
    self.indexes.deinit(self.allocator);
}

fn cleanupDeletedIndexesTask(self: *Self) void {
    self.cleanupDeletedIndexes() catch |err| {
        log.err("cleanup task failed: {}", .{err});
    };

    // Reschedule for next cleanup
    if (self.cleanup_task) |task| {
        self.scheduler.scheduleTask(task);
    }
}

fn cleanupDeletedIndexes(self: *Self) !void {
    log.info("running cleanup of deleted indexes", .{});

    var iter = self.dir.iterate();
    while (try iter.next()) |entry| {
        if (entry.kind != .directory) continue;
        if (!isValidName(entry.name)) continue;

        // Read redirect file
        var cleanup_index_dir = self.dir.openDir(entry.name, .{}) catch continue;
        defer cleanup_index_dir.close();

        const redirect = index_redirect.readRedirectFile(cleanup_index_dir, self.allocator) catch |err| {
            if (err == error.FileNotFound) {
                // No redirect file, check for orphaned data directories
                try self.cleanupOrphanedDataDirs(entry.name);
            }
            continue;
        };

        if (!redirect.deleted) continue;

        // Get data directory name
        const data_dir = try redirect.getDataDir(self.allocator);
        defer self.allocator.free(data_dir);

        // Check if data directory exists and get its manifest age
        var index_dir = self.dir.openDir(entry.name, .{}) catch continue;
        defer index_dir.close();

        var data_subdir = index_dir.openDir(data_dir, .{}) catch {
            // Data directory already gone, just remove redirect
            try self.removeRedirectAndCleanup(entry.name);
            continue;
        };
        defer data_subdir.close();

        const manifest_stat = data_subdir.statFile("manifest") catch {
            // No manifest, safe to delete immediately
            try self.removeRedirectAndCleanup(entry.name);
            continue;
        };

        // Check age of manifest file (1 hour = 3600 seconds)
        const current_time = std.time.timestamp();
        const manifest_age = current_time - manifest_stat.mtime;

        if (manifest_age > 3600) { // 1 hour
            log.info("cleaning up deleted index {s} (manifest age: {}s)", .{ entry.name, manifest_age });
            try self.removeRedirectAndCleanup(entry.name);
        }
    }
}

fn cleanupOrphanedDataDirs(self: *Self, index_name: []const u8) !void {
    var index_dir = self.dir.openDir(index_name, .{}) catch return;
    defer index_dir.close();

    var iter = index_dir.iterate();
    while (try iter.next()) |entry| {
        if (entry.kind != .directory) continue;
        if (!std.mem.startsWith(u8, entry.name, "data.")) continue;

        // Orphaned data directory, check age and remove
        var data_subdir = index_dir.openDir(entry.name, .{}) catch continue;
        defer data_subdir.close();

        const manifest_stat = data_subdir.statFile("manifest") catch {
            // No manifest, safe to delete
            index_dir.deleteTree(entry.name) catch |err| {
                log.warn("failed to delete orphaned data dir {s}/{s}: {}", .{ index_name, entry.name, err });
            };
            continue;
        };

        const current_time = std.time.timestamp();
        const manifest_age = current_time - manifest_stat.mtime;

        if (manifest_age > 3600) { // 1 hour
            log.info("cleaning up orphaned data directory {s}/{s}", .{ index_name, entry.name });
            index_dir.deleteTree(entry.name) catch |err| {
                log.warn("failed to delete orphaned data dir {s}/{s}: {}", .{ index_name, entry.name, err });
            };
        }
    }
}

fn removeRedirectAndCleanup(self: *Self, index_name: []const u8) !void {
    var index_dir = self.dir.openDir(index_name, .{}) catch return;
    defer index_dir.close();

    // Delete redirect file
    index_dir.deleteFile("current") catch |err| {
        if (err != error.FileNotFound) {
            log.warn("failed to delete redirect file for {s}: {}", .{ index_name, err });
        }
    };

    // Delete all data directories
    var iter = index_dir.iterate();
    while (try iter.next()) |entry| {
        if (entry.kind != .directory) continue;
        if (!std.mem.startsWith(u8, entry.name, "data.")) continue;

        index_dir.deleteTree(entry.name) catch |err| {
            log.warn("failed to delete data directory {s}/{s}: {}", .{ index_name, entry.name, err });
        };
    }

    // Try to delete the index directory if it's empty
    self.dir.deleteDir(index_name) catch |err| {
        if (err != error.DirNotEmpty and err != error.FileNotFound) {
            log.warn("failed to delete empty index directory {s}: {}", .{ index_name, err });
        }
    };
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

    const optional_index: *OptionalIndex = @fieldParentPtr("value", index);
    const index_ref: *IndexRef = @fieldParentPtr("index", optional_index);
    const can_delete = index_ref.decRef();
    // the last ref should be held by the internal map and that's only released in deleteIndex
    std.debug.assert(!can_delete);
}

fn borrowIndex(index_ref: *IndexRef) *Index {
    assert(index_ref.index.has_value);
    index_ref.incRef();
    return &index_ref.index.value;
}

pub fn getOrCreateIndex(self: *Self, name: []const u8, create: bool) !*Index {
    if (!isValidName(name)) {
        return error.InvalidIndexName;
    }

    self.lock.lock();
    defer self.lock.unlock();

    if (self.indexes.getEntry(name)) |entry| {
        if (entry.value_ptr.index.has_value) {
            // Index exists and is active
            if (entry.value_ptr.being_deleted) {
                return error.IndexBeingDeleted;
            }
            return borrowIndex(entry.value_ptr);
        }
        // Index is deleted (has_value == false) - fall through to handle recreation if create=true
    }

    if (!create) {
        return error.IndexNotFound;
    }

    log.info("creating index {s}", .{name});

    const index_ref = try self.openIndex(name, create);
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

    // Mark redirect as deleted
    entry.value_ptr.redirect.deleted = true;

    index_redirect.writeRedirectFile(entry.value_ptr.index_dir, entry.value_ptr.redirect) catch |err| {
        entry.value_ptr.incRef();
        log.err("failed to mark redirect as deleted for index {s}: {}", .{ name, err });
        return err;
    };

    // Deinit the Index after successful redirect write
    entry.value_ptr.index.clear();

    // Clear the being_deleted flag - keep the deleted IndexRef in the map for cleanup
    entry.value_ptr.being_deleted = false;
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
