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
const snapshot = @import("snapshot.zig");

const Self = @This();

const DELETE_TIMEOUT_MS = 5000; // 5 seconds timeout for deletion

pub const IndexInfo = struct {
    name: []const u8,
    generation: u64,
    deleted: bool,
};

pub const ListOptions = struct {
    include_deleted: bool = false,
};

pub const IndexOptions = struct {
    generation: ?u64 = null,
    expect_generation: ?u64 = null,
    expect_does_not_exist: bool = false,
    version: ?u64 = null,
};

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
    references: usize = 1,
    delete_files: bool = false,
    being_deleted: bool = false,
    reference_released: std.Thread.Condition = .{},

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

const IndexRefHashMap = std.StringHashMapUnmanaged(*IndexRef);

lock: std.Thread.Mutex = .{},
lock_file: ?std.fs.File = null,
allocator: std.mem.Allocator,
scheduler: *Scheduler,
dir: std.fs.Dir,
index_options: Index.Options,
indexes: IndexRefHashMap = .{},
cleanup_task: ?*Scheduler.Task = null,

fn isValidName(name: []const u8) bool {
    if (name.len == 0) return false;
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
    try std.testing.expect(!isValidName(""));
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

fn openExistingIndex(self: *Self, path: []const u8) !*IndexRef {
    var index_dir = try self.dir.openDir(path, .{});
    errdefer index_dir.close();

    const redirect = index_redirect.readRedirectFile(index_dir, self.allocator) catch |err| {
        log.err("failed to read redirect file: {}", .{err});
        return err;
    };
    errdefer self.allocator.free(redirect.name);

    const name = redirect.name;

    const entry = try self.indexes.getOrPut(self.allocator, name);
    errdefer if (!entry.found_existing) self.indexes.removeByPtr(entry.key_ptr);

    if (entry.found_existing) {
        return error.IndexAlreadyLoaded;
    }

    var ref = try self.allocator.create(IndexRef);
    errdefer self.allocator.destroy(ref);

    entry.value_ptr.* = ref;

    ref.* = .{
        .index_dir = index_dir,
        .redirect = redirect,
    };

    const data_path = try ref.redirect.getDataDir(self.allocator);
    defer self.allocator.free(data_path); // always clean up, index doesn't keep it

    log.info("opening index {s} from {s}/{s}", .{ name, path, data_path });

    ref.index.value = try Index.init(self.allocator, self.scheduler, ref.index_dir, name, data_path, self.index_options);
    ref.index.has_value = true;
    errdefer {
        ref.index.value.deinit();
        ref.index.has_value = false;
    }

    try ref.index.value.open(false);

    return ref;
}

fn createNewIndex(self: *Self, original_name: []const u8, generation: ?u64) !*IndexRef {
    const entry = try self.indexes.getOrPut(self.allocator, original_name);

    const found_existing = entry.found_existing;
    errdefer if (!found_existing) self.indexes.removeByPtr(entry.key_ptr);

    if (!found_existing) {
        // change the key to a newly allocated one
        entry.key_ptr.* = try self.allocator.dupe(u8, original_name);
    }
    errdefer if (!found_existing) self.allocator.free(entry.key_ptr.*);

    const name = entry.key_ptr.*;

    var ref: *IndexRef = undefined;
    if (!found_existing) {
        ref = try self.allocator.create(IndexRef);
        ref.* = .{
            .index_dir = undefined, // will be set later
            .redirect = undefined, // will be set later
        };
        entry.value_ptr.* = ref;
    } else {
        ref = entry.value_ptr.*;
    }
    errdefer if (!found_existing) self.allocator.destroy(ref);

    if (generation) |version| {
        if (found_existing) {
            // Re-creating deleted index - verify version is greater
            if (version <= ref.redirect.version) {
                return error.VersionTooLow;
            }
        }
        ref.redirect = IndexRedirect.init(name, version);
    } else {
        // Original behavior for backward compatibility
        if (!found_existing) {
            ref.redirect = IndexRedirect.init(name, null);
        } else {
            ref.redirect = ref.redirect.nextVersion();
        }
    }
    errdefer ref.redirect.deleted = true;

    if (!found_existing) {
        ref.index_dir = try self.dir.makeOpenPath(name, .{ .iterate = true });
    }
    errdefer if (!found_existing) {
        ref.index_dir.close();
        self.dir.deleteTree(name) catch |err| {
            log.err("failed to clean up index directory {s}: {}", .{ name, err });
        };
    };

    const data_path = try ref.redirect.getDataDir(self.allocator);
    defer self.allocator.free(data_path); // always clean up, index doesn't keep it

    log.info("creating index {s} in {s}/{s}", .{ original_name, name, data_path });

    errdefer {
        ref.index_dir.deleteTree(data_path) catch |err| {
            log.err("failed to clean up index directory {s}/{s}: {}", .{ name, data_path, err });
        };
    }

    ref.index.value = try Index.init(self.allocator, self.scheduler, ref.index_dir, name, data_path, self.index_options);
    ref.index.has_value = true;
    errdefer {
        ref.index.value.deinit();
        ref.index.has_value = false;
    }

    try ref.index.value.open(true);

    log.debug("Index {s} created successfully", .{ref.redirect.name});
    try index_redirect.writeRedirectFile(ref.index_dir, ref.redirect, self.allocator);

    return ref;
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

    self.lock_file = lock_file;
    errdefer self.lock_file = null;

    var iter = self.dir.iterate();
    while (try iter.next()) |entry| {
        if (entry.kind != .directory) {
            continue;
        }
        if (!isValidName(entry.name)) {
            log.warn("skipping unexpected directory {s}", .{entry.name});
            continue;
        }

        _ = self.openExistingIndex(entry.name) catch |err| {
            log.warn("failed to open index {s}: {}", .{ entry.name, err });
        };
    }

    // Schedule periodic cleanup task
    // self.cleanup_task = try self.scheduler.createTask(.low, cleanupDeletedIndexesTask, .{self});
    // if (self.cleanup_task) |task| {
    //     self.scheduler.scheduleTask(task); // Start cleanup immediately
    // }
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
        entry.value_ptr.*.deinit();
        self.allocator.destroy(entry.value_ptr.*);
        self.allocator.free(entry.key_ptr.*);
    }
    self.indexes.deinit(self.allocator);
}

// fn cleanupDeletedIndexesTask(self: *Self) void {
//     self.cleanupDeletedIndexes() catch |err| {
//         log.err("cleanup task failed: {}", .{err});
//     };
//
//     // Reschedule for next cleanup
//     if (self.cleanup_task) |task| {
//         self.scheduler.scheduleTask(task);
//     }
// }

// fn cleanupDeletedIndexes(self: *Self) !void {
//     log.info("running cleanup of deleted indexes", .{});
//
//     var iter = self.dir.iterate();
//     while (try iter.next()) |entry| {
//         if (entry.kind != .directory) continue;
//         if (!isValidName(entry.name)) continue;
//
//         // Read redirect file
//         var cleanup_index_dir = self.dir.openDir(entry.name, .{}) catch continue;
//         defer cleanup_index_dir.close();
//
//         const redirect = index_redirect.readRedirectFile(cleanup_index_dir, self.allocator) catch |err| {
//             if (err == error.FileNotFound) {
//                 // No redirect file, check for orphaned data directories
//                 try self.cleanupOrphanedDataDirs(entry.name);
//             }
//             continue;
//         };
//
//         if (!redirect.deleted) continue;
//
//         // Get data directory name
//         const data_dir = try redirect.getDataDir(self.allocator);
//         defer self.allocator.free(data_dir);
//
//         // Check if data directory exists and get its manifest age
//         var index_dir = self.dir.openDir(entry.name, .{}) catch continue;
//         defer index_dir.close();
//
//         var data_subdir = index_dir.openDir(data_dir, .{}) catch {
//             // Data directory already gone, just remove redirect
//             try self.removeRedirectAndCleanup(entry.name);
//             continue;
//         };
//         defer data_subdir.close();
//
//         const manifest_stat = data_subdir.statFile("manifest") catch {
//             // No manifest, safe to delete immediately
//             try self.removeRedirectAndCleanup(entry.name);
//             continue;
//         };
//
//         // Check age of manifest file (1 hour = 3600 seconds)
//         const current_time = std.time.timestamp();
//         const manifest_age = current_time - manifest_stat.mtime;
//
//         if (manifest_age > 3600) { // 1 hour
//             log.info("cleaning up deleted index {s} (manifest age: {}s)", .{ entry.name, manifest_age });
//             try self.removeRedirectAndCleanup(entry.name);
//         }
//     }
// }

// fn cleanupOrphanedDataDirs(self: *Self, index_name: []const u8) !void {
//     var index_dir = self.dir.openDir(index_name, .{}) catch return;
//     defer index_dir.close();
//
//     var iter = index_dir.iterate();
//     while (try iter.next()) |entry| {
//         if (entry.kind != .directory) continue;
//         if (!std.mem.startsWith(u8, entry.name, "data.")) continue;
//
//         // Orphaned data directory, check age and remove
//         var data_subdir = index_dir.openDir(entry.name, .{}) catch continue;
//         defer data_subdir.close();
//
//         const manifest_stat = data_subdir.statFile("manifest") catch {
//             // No manifest, safe to delete
//             index_dir.deleteTree(entry.name) catch |err| {
//                 log.warn("failed to delete orphaned data dir {s}/{s}: {}", .{ index_name, entry.name, err });
//             };
//             continue;
//         };
//
//         const current_time = std.time.timestamp();
//         const manifest_age = current_time - manifest_stat.mtime;
//
//         if (manifest_age > 3600) { // 1 hour
//             log.info("cleaning up orphaned data directory {s}/{s}", .{ index_name, entry.name });
//             index_dir.deleteTree(entry.name) catch |err| {
//                 log.warn("failed to delete orphaned data dir {s}/{s}: {}", .{ index_name, entry.name, err });
//             };
//         }
//     }
// }

// fn removeRedirectAndCleanup(self: *Self, index_name: []const u8) !void {
//     var index_dir = self.dir.openDir(index_name, .{}) catch return;
//     defer index_dir.close();
//
//     // Delete redirect file
//     index_dir.deleteFile("current") catch |err| {
//         if (err != error.FileNotFound) {
//             log.warn("failed to delete redirect file for {s}: {}", .{ index_name, err });
//         }
//     };
//
//     // Delete all data directories
//     var iter = index_dir.iterate();
//     while (try iter.next()) |entry| {
//         if (entry.kind != .directory) continue;
//         if (!std.mem.startsWith(u8, entry.name, "data.")) continue;
//
//         index_dir.deleteTree(entry.name) catch |err| {
//             log.warn("failed to delete data directory {s}/{s}: {}", .{ index_name, entry.name, err });
//         };
//     }
//
//     // Try to delete the index directory if it's empty
//     self.dir.deleteDir(index_name) catch |err| {
//         if (err != error.DirNotEmpty and err != error.FileNotFound) {
//             log.warn("failed to delete empty index directory {s}: {}", .{ index_name, err });
//         }
//     };
// }

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

    // Notify any waiting deleteIndex operations
    index_ref.reference_released.broadcast();
}

fn borrowIndex(index_ref: *IndexRef) *Index {
    assert(index_ref.index.has_value);
    index_ref.incRef();
    return &index_ref.index.value;
}

pub fn getOrCreateIndex(self: *Self, name: []const u8, create: bool, options: IndexOptions) !*Index {
    self.lock.lock();
    defer self.lock.unlock();

    if (self.indexes.get(name)) |index_ref| {
        if (index_ref.index.has_value) {
            // Index exists and is active
            if (index_ref.being_deleted) {
                return error.IndexBeingDeleted;
            }
            if (options.expect_does_not_exist) {
                return error.IndexAlreadyExists;
            }
            // Validate expected generation if provided
            if (options.expect_generation) |expect_generation| {
                if (index_ref.redirect.version != expect_generation) {
                    return error.IndexGenerationMismatch;
                }
            }
            return borrowIndex(index_ref);
        }
        // Index is deleted (has_value == false) - fall through to handle recreation if create=true
    }

    if (!create) {
        return error.IndexNotFound;
    }

    const index_ref = try self.createNewIndex(name, options.generation);
    return borrowIndex(index_ref);
}

pub fn getIndex(self: *Self, name: []const u8) !*Index {
    return self.getOrCreateIndex(name, false, .{});
}

pub fn deleteIndex(self: *Self, name: []const u8, request: api.DeleteIndexRequest) !void {
    if (!isValidName(name)) {
        return error.InvalidIndexName;
    }

    self.lock.lock();
    defer self.lock.unlock();

    const index_ref = self.indexes.get(name) orelse {
        if (request.expect_exists) {
            return error.IndexNotFound;
        }
        return;
    };
    if (!index_ref.index.has_value) {
        if (request.expect_exists) {
            return error.IndexNotFound;
        }
        return;
    }


    // Mark as being deleted to prevent new references
    if (index_ref.being_deleted) {
        return error.IndexAlreadyBeingDeleted;
    }
    index_ref.being_deleted = true;
    defer index_ref.being_deleted = false;

    log.info("marking index {s} for deletion, waiting for references to be released", .{name});

    // Wait for all references except the map's reference to be released
    var timer = try std.time.Timer.start();
    while (index_ref.references > 1) {
        const elapsed_ms = timer.read() / std.time.ns_per_ms;
        if (elapsed_ms > DELETE_TIMEOUT_MS) {
            log.warn("timeout waiting for index {s} references to be released (current: {d})", .{ name, index_ref.references });
            return error.DeleteTimeout;
        }

        // Wait on condition variable with remaining timeout
        const remaining_timeout_ns = (DELETE_TIMEOUT_MS - elapsed_ms) * std.time.ns_per_ms;
        index_ref.reference_released.timedWait(&self.lock, remaining_timeout_ns) catch {
            // Timeout occurred, continue the loop to check references again
        };

        // Check if the entry still exists (in case of concurrent operations)
        const current_index_ref = self.indexes.get(name) orelse return error.IndexNotFound;
        if (current_index_ref != index_ref) {
            return error.IndexNotFound;
        }
    }

    log.info("deleting index {s}", .{name});

    // Update redirect with new version and mark as deleted
    if (request.generation) |generation| {
        if (generation <= index_ref.redirect.version) {
            return error.VersionTooLow;
        }
        index_ref.redirect.version = generation;
    } else {
        index_ref.redirect.version += 1;
    }
    index_ref.redirect.deleted = true;
    errdefer {
        index_ref.redirect.deleted = false;
        if (request.generation == null) {
            index_ref.redirect.version -= 1;
        }
    }

    index_redirect.writeRedirectFile(index_ref.index_dir, index_ref.redirect, self.allocator) catch |err| {
        log.err("failed to mark redirect as deleted for index {s}: {}", .{ name, err });
        return err;
    };

    // Close the index
    index_ref.index.clear();
}


pub fn search(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
    request: api.SearchRequest,
) !api.SearchResponse {
    if (!isValidName(index_name)) {
        return error.InvalidIndexName;
    }
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

pub fn updateInternal(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
    request: api.UpdateRequest,
    options: IndexOptions,
) !api.UpdateResponse {
    if (!isValidName(index_name)) {
        return error.InvalidIndexName;
    }
    _ = allocator; // Response doesn't need allocation

    const index = try self.getOrCreateIndex(index_name, false, options);
    defer self.releaseIndex(index);

    metrics.update(request.changes.len);

    const new_version = try index.update(
        request.changes,
        request.metadata,
        .{
            .expected_last_version = request.expected_version,
            .version = options.version,
        },
    );

    return api.UpdateResponse{ .version = new_version };
}

pub fn update(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
    request: api.UpdateRequest,
) !api.UpdateResponse {
    return self.updateInternal(allocator, index_name, request, .{});
}

pub fn getIndexInfo(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
) !api.GetIndexInfoResponse {
    if (!isValidName(index_name)) {
        return error.InvalidIndexName;
    }
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
    if (!isValidName(index_name)) {
        return error.InvalidIndexName;
    }
    const index = try self.getIndex(index_name);
    defer self.releaseIndex(index);
    // Just checking existence, no need to return anything
}

pub fn createIndex(
    self: *Self,
    index_name: []const u8,
    request: api.CreateIndexRequest,
) !api.CreateIndexResponse {
    if (!isValidName(index_name)) {
        return error.InvalidIndexName;
    }

    const options = IndexOptions{
        .expect_does_not_exist = request.expect_does_not_exist,
        .generation = request.generation,
    };

    const index = try self.getOrCreateIndex(index_name, true, options);
    defer self.releaseIndex(index);

    var index_reader = try index.acquireReader();
    defer index.releaseReader(&index_reader);

    return api.CreateIndexResponse{
        .version = index_reader.getVersion(),
        .ready = true,
    };
}

pub fn getFingerprintInfo(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
    fingerprint_id: u32,
) !api.GetFingerprintInfoResponse {
    if (!isValidName(index_name)) {
        return error.InvalidIndexName;
    }
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
    if (!isValidName(index_name)) {
        return error.InvalidIndexName;
    }
    const index = try self.getIndex(index_name);
    defer self.releaseIndex(index);

    var index_reader = try index.acquireReader();
    defer index.releaseReader(&index_reader);

    const info = try index_reader.getDocInfo(fingerprint_id);
    if (info == null) {
        return error.FingerprintNotFound;
    }
}

pub fn getLocalIndexInfo(self: *Self, name: []const u8) ?IndexInfo {
    self.lock.lock();
    defer self.lock.unlock();

    const index_ref = self.indexes.get(name) orelse return null;

    return IndexInfo{
        .name = name,
        .generation = index_ref.redirect.version,
        .deleted = !index_ref.index.has_value,
    };
}

pub fn listIndexes(self: *Self, allocator: std.mem.Allocator, options: ListOptions) ![]IndexInfo {
    self.lock.lock();
    defer self.lock.unlock();

    var result = std.ArrayList(IndexInfo).init(allocator);
    defer result.deinit();

    var iter = self.indexes.iterator();
    while (iter.next()) |entry| {
        const name = entry.key_ptr.*;
        const index_ref = entry.value_ptr.*;

        // Check if index is deleted
        const deleted = !index_ref.index.has_value;

        // Skip deleted indexes if not requested
        if (deleted and !options.include_deleted) {
            continue;
        }

        // Get generation from redirect
        const generation = index_ref.redirect.version;

        try result.append(IndexInfo{
            .name = name,
            .generation = generation,
            .deleted = deleted,
        });
    }

    return result.toOwnedSlice();
}

pub fn exportSnapshot(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
    writer: anytype,
) !void {
    if (!isValidName(index_name)) {
        return error.InvalidIndexName;
    }

    const index = try self.getIndex(index_name);
    defer self.releaseIndex(index);

    try snapshot.buildSnapshot(writer, index, allocator);
}

const TestContext = struct {
    tmp_dir: std.testing.TmpDir = undefined,
    scheduler: Scheduler = undefined,
    indexes: Self = undefined,

    pub fn setup(ctx: *TestContext) !void {
        ctx.tmp_dir = std.testing.tmpDir(.{});
        ctx.scheduler = Scheduler.init(std.testing.allocator);
        ctx.indexes = Self.init(std.testing.allocator, &ctx.scheduler, ctx.tmp_dir.dir, .{});

        try ctx.scheduler.start(2);
    }

    pub fn teardown(ctx: *TestContext) void {
        ctx.indexes.deinit();
        ctx.scheduler.deinit();
        ctx.tmp_dir.cleanup();
    }
};

test "setup" {
    var ctx: TestContext = .{};
    try ctx.setup();
    defer ctx.teardown();
}

test "createIndex" {
    var ctx: TestContext = .{};
    try ctx.setup();
    defer ctx.teardown();

    const info = try ctx.indexes.createIndex("foo", .{});
    try std.testing.expectEqual(0, info.version);
    try ctx.indexes.checkIndexExists("foo");
}

test "createIndex twice" {
    var ctx: TestContext = .{};
    try ctx.setup();
    defer ctx.teardown();

    const info = try ctx.indexes.createIndex("foo", .{});
    try std.testing.expectEqual(0, info.version);
    try ctx.indexes.checkIndexExists("foo");

    const info2 = try ctx.indexes.createIndex("foo", .{});
    try std.testing.expectEqual(0, info2.version);
    try ctx.indexes.checkIndexExists("foo");
}

test "deleteIndex" {
    var ctx: TestContext = .{};
    try ctx.setup();
    defer ctx.teardown();

    const info = try ctx.indexes.createIndex("foo", .{});
    try std.testing.expectEqual(0, info.version);
    try ctx.indexes.checkIndexExists("foo");

    try ctx.indexes.deleteIndex("foo", .{});
    try std.testing.expectError(error.IndexNotFound, ctx.indexes.checkIndexExists("foo"));
}

test "deleteIndex twice" {
    var ctx: TestContext = .{};
    try ctx.setup();
    defer ctx.teardown();

    const info = try ctx.indexes.createIndex("foo", .{});
    try std.testing.expectEqual(0, info.version);
    try ctx.indexes.checkIndexExists("foo");

    try ctx.indexes.deleteIndex("foo", .{});
    try std.testing.expectError(error.IndexNotFound, ctx.indexes.checkIndexExists("foo"));

    try ctx.indexes.deleteIndex("foo", .{});
    try std.testing.expectError(error.IndexNotFound, ctx.indexes.checkIndexExists("foo"));
}

test "update" {
    var ctx: TestContext = .{};
    try ctx.setup();
    defer ctx.teardown();

    const Change = @import("change.zig").Change;

    const info = try ctx.indexes.createIndex("foo", .{});
    try std.testing.expectEqual(0, info.version);

    var changes = [_]Change{
        .{ .insert = .{ .id = 1, .hashes = &[_]u32{ 1, 2, 3 } } },
    };

    const result = try ctx.indexes.update(std.testing.allocator, "foo", .{ .changes = &changes });
    try std.testing.expectEqual(1, result.version);
}

test "update with custom version" {
    var ctx: TestContext = .{};
    try ctx.setup();
    defer ctx.teardown();

    const Change = @import("change.zig").Change;

    const info = try ctx.indexes.createIndex("foo", .{});
    try std.testing.expectEqual(0, info.version);

    var changes = [_]Change{
        .{ .insert = .{ .id = 1, .hashes = &[_]u32{ 1, 2, 3 } } },
    };

    // Test with custom version
    const result1 = try ctx.indexes.updateInternal(std.testing.allocator, "foo", .{ .changes = &changes }, .{ .version = 100 });
    try std.testing.expectEqual(100, result1.version);

    // Test with another custom version
    const result2 = try ctx.indexes.updateInternal(std.testing.allocator, "foo", .{ .changes = &changes }, .{ .version = 200 });
    try std.testing.expectEqual(200, result2.version);

    // Test that monotonicity is enforced
    const result_error = ctx.indexes.updateInternal(std.testing.allocator, "foo", .{ .changes = &changes }, .{ .version = 150 });
    try std.testing.expectError(error.VersionNotMonotonic, result_error);
}
