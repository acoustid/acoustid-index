const std = @import("std");
const log = std.log.scoped(.multi_index);
const assert = std.debug.assert;

const Index = @import("Index.zig");
const Scheduler = @import("utils/Scheduler.zig");
const restoration = @import("restoration.zig");

const Self = @This();

pub const IndexRef = struct {
    index: Index,
    name: []const u8,
    references: usize = 0,
    last_used_at: i64 = std.math.minInt(i64),

    pub fn deinit(self: *IndexRef, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        self.index.deinit();
    }

    pub fn incRef(self: *IndexRef) void {
        self.references += 1;
        self.last_used_at = std.time.milliTimestamp();
    }

    pub fn decRef(self: *IndexRef) bool {
        assert(self.references > 0);
        self.references -= 1;
        self.last_used_at = std.time.milliTimestamp();
        return self.references == 0;
    }
};

lock: std.Thread.Mutex = .{},
allocator: std.mem.Allocator,
scheduler: *Scheduler,
dir: std.fs.Dir,
index_options: Index.Options,
indexes: std.StringHashMap(IndexRef),

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
        .indexes = std.StringHashMap(IndexRef).init(allocator),
    };
}

pub fn deinit(self: *Self) void {
    self.lock.lock();
    defer self.lock.unlock();

    var iter = self.indexes.iterator();
    while (iter.next()) |entry| {
        self.allocator.free(entry.key_ptr.*);
        entry.value_ptr.index.deinit();
    }
    self.indexes.deinit();
}

fn deleteIndexFiles(self: *Self, name: []const u8) !void {
    const tmp_name = try std.mem.concat(self.allocator, u8, &[_][]const u8{ name, ".delete" });
    defer self.allocator.free(tmp_name);
    self.dir.rename(name, tmp_name) catch |err| {
        if (err == error.FileNotFound) {
            return;
        }
        return err;
    };
    try self.dir.deleteTree(tmp_name);
}

fn removeIndex(self: *Self, name: []const u8) void {
    if (self.indexes.getEntry(name)) |entry| {
        entry.value_ptr.deinit(self.allocator);
        self.indexes.removeByPtr(entry.key_ptr);
    }
}

fn releaseIndexRef(self: *Self, index_ref: *IndexRef) void {
    self.lock.lock();
    defer self.lock.unlock();

    _ = index_ref.decRef();
}

pub fn releaseIndex(self: *Self, index: *Index) void {
    self.releaseIndexRef(@fieldParentPtr("index", index));
}

fn acquireIndex(self: *Self, name: []const u8, create: bool) !*IndexRef {
    return self.acquireIndexWithRestore(name, create, null);
}

fn acquireIndexWithRestore(self: *Self, name: []const u8, create: bool, restore_source: ?restoration.RestoreSource) !*IndexRef {
    if (!isValidName(name)) {
        return error.InvalidIndexName;
    }

    self.lock.lock();
    defer self.lock.unlock();

    var result = try self.indexes.getOrPutAdapted(name, self.indexes.ctx);
    if (result.found_existing) {
        result.value_ptr.incRef();
        return result.value_ptr;
    }
    errdefer self.indexes.removeByPtr(result.key_ptr);

    result.key_ptr.* = try self.allocator.dupe(u8, name);
    errdefer self.allocator.free(result.key_ptr.*);

    result.value_ptr.* = .{
        .index = try Index.init(self.allocator, self.scheduler, self.dir, result.key_ptr.*, self.index_options),
        .name = result.key_ptr.*,
    };
    errdefer result.value_ptr.index.deinit();

    try result.value_ptr.index.openWithRestore(create, restore_source);

    result.value_ptr.incRef();
    return result.value_ptr;
}

pub fn getIndex(self: *Self, name: []const u8) !*Index {
    const index_ref = try self.acquireIndex(name, false);
    errdefer self.releaseIndexRef(index_ref);

    return &index_ref.index;
}

pub fn createIndex(self: *Self, name: []const u8) !*Index {
    log.info("creating index {s}", .{name});

    const index_ref = try self.acquireIndex(name, true);
    errdefer self.releaseIndexRef(index_ref);

    return &index_ref.index;
}

pub fn createIndexWithRestore(self: *Self, name: []const u8, restore_source: restoration.RestoreSource) !*Index {
    log.info("creating index {s} from restoration", .{name});

    if (self.indexExists(name)) {
        return error.IndexAlreadyExists;
    }

    const index_ref = try self.acquireIndexWithRestore(name, true, restore_source);
    errdefer self.releaseIndexRef(index_ref);

    return &index_ref.index;
}

pub fn deleteIndex(self: *Self, name: []const u8) !void {
    log.info("deleting index {s}", .{name});

    if (!isValidName(name)) {
        return error.InvalidIndexName;
    }

    self.lock.lock();
    defer self.lock.unlock();

    if (self.indexes.getEntry(name)) |entry| {
        entry.value_ptr.index.deinit();
        self.allocator.free(entry.key_ptr.*);
        self.indexes.removeByPtr(entry.key_ptr);
    }

    try self.deleteIndexFiles(name);
}

pub fn indexExists(self: *Self, name: []const u8) bool {
    self.lock.lock();
    defer self.lock.unlock();
    
    return self.indexes.contains(name);
}

pub fn getIndexDir(self: *Self, name: []const u8) !std.fs.Dir {
    if (!isValidName(name)) {
        return error.InvalidIndexName;
    }
    
    return try self.dir.makeOpenPath(name, .{});
}
