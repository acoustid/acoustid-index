const std = @import("std");
const log = std.log.scoped(.multi_index);
const assert = std.debug.assert;

const Index = @import("Index.zig");

const Self = @This();

pub const IndexRef = struct {
    index: Index,
    references: usize = 0,
    last_used_at: i64 = std.math.minInt(i64),
};

lock: std.Thread.Mutex = .{},
allocator: std.mem.Allocator,
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

pub fn init(allocator: std.mem.Allocator, dir: std.fs.Dir) Self {
    return .{
        .allocator = allocator,
        .dir = dir,
        .indexes = std.StringHashMap(IndexRef).init(allocator),
    };
}

pub fn deinit(self: *Self) void {
    var iter = self.indexes.iterator();
    while (iter.next()) |entry| {
        self.allocator.free(entry.key_ptr.*);
        entry.value_ptr.index.deinit();
    }
    self.indexes.deinit();
}

pub fn releaseIndex(self: *Self, index_data: *IndexRef) void {
    self.lock.lock();
    defer self.lock.unlock();

    assert(index_data.references > 0);
    index_data.references -= 1;
    index_data.last_used_at = std.time.timestamp();
}

pub fn acquireIndex(self: *Self, name: []const u8, create: bool) !*IndexRef {
    if (!isValidName(name)) {
        return error.InvalidIndexName;
    }

    self.lock.lock();
    defer self.lock.unlock();

    var result = try self.indexes.getOrPutAdapted(name, self.indexes.ctx);
    if (result.found_existing) {
        result.value_ptr.references += 1;
        result.value_ptr.last_used_at = std.time.timestamp();
        return result.value_ptr;
    }

    errdefer self.indexes.removeByPtr(result.key_ptr);

    result.key_ptr.* = try self.allocator.dupe(u8, name);
    errdefer self.allocator.free(result.key_ptr.*);

    result.value_ptr.index = try Index.init(self.allocator, self.dir, name, .{ .create = create });
    errdefer result.value_ptr.index.deinit();

    try result.value_ptr.index.open();

    result.value_ptr.references = 1;
    result.value_ptr.last_used_at = std.time.timestamp();
    return result.value_ptr;
}

pub fn getIndex(self: *Self, name: []const u8) !*IndexRef {
    if (!isValidName(name)) {
        return error.InvalidIndexName;
    }

    return try self.acquireIndex(name, false);
}

pub fn createIndex(self: *Self, name: []const u8) !void {
    if (!isValidName(name)) {
        return error.InvalidIndexName;
    }

    const index_ref = try self.acquireIndex(name, true);
    defer self.releaseIndex(index_ref);
}

pub fn deleteIndex(self: *Self, name: []const u8) !void {
    if (!isValidName(name)) {
        return error.InvalidIndexName;
    }

    self.lock.lock();
    defer self.lock.unlock();

    if (self.indexes.getEntry(name)) |entry| {
        self.allocator.free(entry.key_ptr.*);
        entry.value_ptr.index.deinit();
        self.indexes.removeByPtr(entry.key_ptr);
    }

    try self.dir.deleteTree(name);
}
