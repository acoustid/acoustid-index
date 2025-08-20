const std = @import("std");
const log = std.log.scoped(.multi_index);
const assert = std.debug.assert;

const Index = @import("Index.zig");
const Scheduler = @import("utils/Scheduler.zig");

const Self = @This();

pub const IndexRef = struct {
    index: Index,
    references: usize = 0,
    delete_files: bool = false,

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

fn releaseIndexRef(self: *Self, index_ref: *IndexRef) bool {
    const delete = index_ref.decRef();
    if (delete) {
        log.info("deinit on index", .{});
        index_ref.index.deinit();
        self.allocator.free(index_ref.index.name);
    }
    return delete;
}

pub fn releaseIndex(self: *Self, index: *Index) void {
    self.lock.lock();
    defer self.lock.unlock();

    _ = self.releaseIndexRef(@fieldParentPtr("index", index));
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

pub fn createIndex(self: *Self, name: []const u8) !*Index {
    return self.getOrCreateIndex(name, true);
}

pub fn deleteIndex(self: *Self, name: []const u8) !void {
    if (!isValidName(name)) {
        return error.InvalidIndexName;
    }

    self.lock.lock();
    defer self.lock.unlock();

    const entry = self.indexes.getEntry(name) orelse return;

    log.info("deleting index {s}", .{name});

    const deleted = self.releaseIndexRef(entry.value_ptr);
    if (!deleted) {
        entry.value_ptr.incRef();
        return error.IndexInUse;
    }

    self.deleteIndexFiles(name) catch |err| {
        entry.value_ptr.incRef();
        return err;
    };

    self.indexes.removeByPtr(entry.key_ptr);
}
