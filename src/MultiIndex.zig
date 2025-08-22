const std = @import("std");
const log = std.log.scoped(.multi_index);
const assert = std.debug.assert;

const Index = @import("Index.zig");
const Scheduler = @import("utils/Scheduler.zig");

const Self = @This();

const DELETE_TIMEOUT_MS = 5000; // 5 seconds timeout for deletion

pub const IndexState = enum {
    ready,
    restoring,
    error_state,
};

pub const IndexRef = struct {
    index: Index,
    references: usize = 0,
    delete_files: bool = false,
    being_deleted: bool = false,
    state: IndexState = .ready,
    restore_task: ?Scheduler.Task = null,

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
    switch (index_ref.state) {
        .ready => {},
        .restoring => return error.IndexNotReady,
        .error_state => return error.IndexRestoreFailed,
    }
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

    // Create with no restore options (unlocked version)
    return self.createIndexUnlocked(name, .{});
}

fn createIndexUnlocked(self: *Self, name: []const u8, options: CreateIndexOptions) !*Index {
    // This is the internal version that assumes the lock is already held
    if (self.indexes.getEntry(name)) |entry| {
        if (entry.value_ptr.being_deleted) {
            return error.IndexBeingDeleted;
        }
        // Always return error when index already exists - server maps this to 409
        return error.IndexAlreadyExists;
    }

    if (options.restore) |restore_opts| {
        // Create a placeholder entry in restoring state
        const name_copy = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(name_copy);

        var result = try self.indexes.getOrPut(self.allocator, name_copy);
        if (result.found_existing) {
            self.allocator.free(name_copy);
            unreachable; // We already checked above
        }
        errdefer self.indexes.removeByPtr(result.key_ptr);

        // Initialize with placeholder index and restoring state
        result.value_ptr.* = .{
            .index = undefined, // Will be initialized after restoration
            .references = 1,
            .state = .restoring,
        };

        // Deep-copy restore options off the request arena
        const host_copy = try self.allocator.dupe(u8, restore_opts.host);
        errdefer self.allocator.free(host_copy);
        const index_name_copy = try self.allocator.dupe(u8, restore_opts.index_name);
        errdefer self.allocator.free(index_name_copy);
        const ro_copy = CreateIndexOptions.RestoreOptions{
            .host = host_copy,
            .port = restore_opts.port,
            .index_name = index_name_copy,
        };

        // Schedule restoration task with owned options
        const restore_task = try self.scheduler.createTask(.medium, restoreIndexTask, .{ self, name_copy, ro_copy });
        result.value_ptr.restore_task = restore_task;
        self.scheduler.scheduleTask(restore_task);

        log.info("scheduled restoration for index {s}", .{name});
        return error.IndexNotReady; // Caller should handle this as 202 Accepted
    }

    log.info("creating index {s}", .{name});
    const index_ref = try self.openIndex(name, true);
    return borrowIndex(index_ref);
}

pub fn getIndex(self: *Self, name: []const u8) !*Index {
    return self.getOrCreateIndex(name, false);
}

pub const CreateIndexOptions = struct {
    restore: ?RestoreOptions = null,
    
    pub const RestoreOptions = struct {
        host: []const u8,
        port: u16,
        index_name: []const u8,
        
        pub fn msgpackFormat() @import("msgpack").StructFormat {
            return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
        }
    };
};

pub fn createIndex(self: *Self, name: []const u8, options: CreateIndexOptions) !*Index {
    if (!isValidName(name)) {
        return error.InvalidIndexName;
    }

    self.lock.lock();
    defer self.lock.unlock();

    return self.createIndexUnlocked(name, options);
}

fn restoreIndexTask(self: *Self, name: []const u8, restore_opts: CreateIndexOptions.RestoreOptions) void {
    // Ensure we free the deep-copied slices
    defer self.allocator.free(restore_opts.host);
    defer self.allocator.free(restore_opts.index_name);

    self.restoreIndexFromHttp(name, restore_opts) catch |err| {
        log.err("failed to restore index {s}: {}", .{ name, err });
        self.markIndexRestoreFailed(name);
        return;
    };
    
    self.markIndexRestoreComplete(name) catch |err| {
        log.err("failed to complete index restoration for {s}: {}", .{ name, err });
        self.markIndexRestoreFailed(name);
        return;
    };
}

fn markIndexRestoreFailed(self: *Self, name: []const u8) void {
    self.lock.lock();
    defer self.lock.unlock();
    
    if (self.indexes.getEntry(name)) |entry| {
        entry.value_ptr.state = .error_state;
        // Don't destroy task from within task execution - just clear the reference
        entry.value_ptr.restore_task = null;
    }
}

fn markIndexRestoreComplete(self: *Self, name: []const u8) !void {
    self.lock.lock();
    defer self.lock.unlock();
    
    const entry = self.indexes.getEntry(name) orelse return error.IndexNotFound;
    
    // Initialize the actual index now that files are restored
    entry.value_ptr.index = try Index.init(self.allocator, self.scheduler, self.dir, name, self.index_options);
    errdefer entry.value_ptr.index.deinit();
    
    try entry.value_ptr.index.open(false); // open existing index
    
    // Mark as ready
    entry.value_ptr.state = .ready;
    
    // Don't destroy task from within task execution - just clear the reference
    entry.value_ptr.restore_task = null;
    
    log.info("index {s} restoration completed successfully", .{name});
}

fn restoreIndexFromHttp(self: *Self, name: []const u8, restore_opts: CreateIndexOptions.RestoreOptions) !void {
    log.info("restoring index {s} from http://{s}:{d}/{s}/_snapshot", .{ name, restore_opts.host, restore_opts.port, restore_opts.index_name });

    // Validate inputs
    if (restore_opts.port == 0) return error.InvalidPort;
    if (!isValidName(restore_opts.index_name)) return error.InvalidIndexName;
    
    // Basic SSRF protection - block obvious loopback and private addresses
    if (std.ascii.eqlIgnoreCase(restore_opts.host, "localhost") or
        std.mem.startsWith(u8, restore_opts.host, "127.") or
        std.mem.eql(u8, restore_opts.host, "::1") or
        std.mem.startsWith(u8, restore_opts.host, "10.") or
        std.mem.startsWith(u8, restore_opts.host, "192.168.") or
        std.mem.startsWith(u8, restore_opts.host, "169.254.") or
        std.mem.startsWith(u8, restore_opts.host, "172.")) {
        return error.ForbiddenHost;
    }

    // Create HTTP client
    var client = std.http.Client{ .allocator = self.allocator };
    defer client.deinit();

    // Handle IPv6 addresses - bracket them if needed
    var host_buf: ?[]u8 = null;
    defer if (host_buf) |hb| self.allocator.free(hb);
    const needs_brackets = std.mem.indexOfScalar(u8, restore_opts.host, ':') != null and
        !(restore_opts.host.len >= 2 and restore_opts.host[0] == '[' and restore_opts.host[restore_opts.host.len - 1] == ']');
    const host_for_url = blk: {
        if (needs_brackets) {
            host_buf = try std.mem.concat(self.allocator, u8, &[_][]const u8{ "[", restore_opts.host, "]" });
            break :blk host_buf.?;
        } else break :blk restore_opts.host;
    };

    // Build URL
    const url = try std.fmt.allocPrint(self.allocator, "http://{s}:{d}/{s}/_snapshot", .{ host_for_url, restore_opts.port, restore_opts.index_name });
    defer self.allocator.free(url);

    // Allocate server header buffer
    const server_header_buffer = try self.allocator.alloc(u8, 8192);
    defer self.allocator.free(server_header_buffer);

    // Create HTTP request
    var req = try client.open(.GET, try std.Uri.parse(url), .{
        .server_header_buffer = server_header_buffer,
    });
    defer req.deinit();

    // Note: Accept header would be set here if the API supports it
    // try req.headers.append("Accept", "application/x-tar");

    try req.send();
    try req.finish();
    try req.wait();

    if (req.response.status != .ok) {
        log.err("HTTP request failed with status: {}", .{req.response.status});
        return error.HttpRequestFailed;
    }

    // Create temporary directory for extraction with unique name
    const tmp_dir_name = try std.fmt.allocPrint(self.allocator, "{s}.restore-{d}", .{ name, std.time.milliTimestamp() });
    defer self.allocator.free(tmp_dir_name);

    // Clean up any previous temp dir with the same name
    self.dir.deleteTree(tmp_dir_name) catch |err| {
        if (err != error.FileNotFound) {
            log.warn("failed to remove old temporary directory {s}: {}", .{ tmp_dir_name, err });
        }
    };

    var tmp_dir = self.dir.makeOpenPath(tmp_dir_name, .{}) catch |err| {
        log.err("failed to create temporary directory {s}: {}", .{ tmp_dir_name, err });
        return err;
    };
    defer tmp_dir.close();
    defer self.dir.deleteTree(tmp_dir_name) catch {};

    // Enforce maximum snapshot size (4GB limit) to prevent tarbombs
    const MAX_SNAPSHOT_BYTES: u64 = 4 * 1024 * 1024 * 1024;
    
    // Extract tar with size limit to prevent disk exhaustion
    var limited = std.io.limitedReader(req.reader(), MAX_SNAPSHOT_BYTES);
    try std.tar.pipeToFileSystem(tmp_dir, limited.reader(), .{});

    // Move temporary directory to final location
    self.dir.rename(tmp_dir_name, name) catch |err| {
        log.err("failed to move temporary directory {s} to {s}: {}", .{ tmp_dir_name, name, err });
        return err;
    };

    log.info("successfully restored index {s}", .{name});
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
    
    // Cannot delete index while it's being restored
    if (entry.value_ptr.state == .restoring) {
        return error.IndexNotReady;
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

    // Clean up restore task if it exists
    if (entry.value_ptr.restore_task) |task| {
        self.scheduler.destroyTask(task);
        entry.value_ptr.restore_task = null;
    }

    self.deleteIndexFiles(name) catch |err| {
        entry.value_ptr.incRef();
        return err;
    };

    // Clear the being_deleted flag since we're about to remove the entry
    entry.value_ptr.being_deleted = false;

    // Ensure Index can safely log/use the name during deinit
    const key_mem = entry.key_ptr.*;
    // Only deinit the index if it's in ready state (not restoring)
    if (entry.value_ptr.state == .ready) {
        entry.value_ptr.index.deinit();
    }
    self.indexes.removeByPtr(entry.key_ptr);
    self.allocator.free(key_mem);
}
