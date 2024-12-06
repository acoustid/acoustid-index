const std = @import("std");
const log = std.log.scoped(.multi_index);
const assert = std.debug.assert;

const Index = @import("Index.zig");
const Scheduler = @import("utils/Scheduler.zig");

const Self = @This();

pub const IndexRef = struct {
    index: Index,
    name: []const u8,
    references: usize = 0,
    last_used_at: i64 = std.math.minInt(i64),
    is_open: bool = false,
    lock: std.Thread.Mutex = .{},

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

    pub fn ensureOpen(self: *IndexRef, create: bool) !void {
        self.lock.lock();
        defer self.lock.unlock();

        if (self.is_open) return;

        try self.index.open(create);
        self.is_open = true;
    }
};

lock: std.Thread.Mutex = .{},
allocator: std.mem.Allocator,
scheduler: *Scheduler,
dir: std.fs.Dir,
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

pub fn init(allocator: std.mem.Allocator, scheduler: *Scheduler, dir: std.fs.Dir) Self {
    return .{
        .allocator = allocator,
        .scheduler = scheduler,
        .dir = dir,
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

fn acquireIndex(self: *Self, name: []const u8) !*IndexRef {
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
        .index = try Index.init(self.allocator, self.scheduler, self.dir, name, .{}),
        .name = result.key_ptr.*,
    };
    errdefer result.value_ptr.index.deinit();

    result.value_ptr.incRef();
    return result.value_ptr;
}

pub fn getIndex(self: *Self, name: []const u8) !*Index {
    const index_ref = try self.acquireIndex(name);
    errdefer self.releaseIndexRef(index_ref);

    try index_ref.ensureOpen(false);

    return &index_ref.index;
}

pub fn createIndex(self: *Self, name: []const u8) !void {
    log.info("creating index {s}", .{name});

    const index_ref = try self.acquireIndex(name);
    defer self.releaseIndexRef(index_ref);

    try index_ref.ensureOpen(true);
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
