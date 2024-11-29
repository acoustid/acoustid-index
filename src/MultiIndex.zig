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
indexes: std.AutoHashMap(u8, IndexRef),

const max_sub_dir_name_size = 10;
const sub_dir_name_fmt = "{x:0>2}";

pub fn init(allocator: std.mem.Allocator, dir: std.fs.Dir) Self {
    return .{
        .allocator = allocator,
        .dir = dir,
        .indexes = std.AutoHashMap(u8, IndexRef).init(allocator),
    };
}

pub fn deinit(self: *Self) void {
    var iter = self.indexes.iterator();
    while (iter.next()) |entry| {
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

pub fn acquireIndex(self: *Self, id: u8, create: bool) !*IndexRef {
    self.lock.lock();
    defer self.lock.unlock();

    var result = try self.indexes.getOrPut(id);
    if (result.found_existing) {
        result.value_ptr.references += 1;
        result.value_ptr.last_used_at = std.time.timestamp();
        return result.value_ptr;
    }

    errdefer self.indexes.removeByPtr(result.key_ptr);

    var sub_dir_name_buf: [max_sub_dir_name_size]u8 = undefined;
    const sub_dir_name = try std.fmt.bufPrint(&sub_dir_name_buf, sub_dir_name_fmt, .{id});

    result.value_ptr.index = try Index.init(self.allocator, self.dir, sub_dir_name, .{ .create = create });
    errdefer result.value_ptr.index.deinit();

    try result.value_ptr.index.open();

    result.value_ptr.references = 1;
    result.value_ptr.last_used_at = std.time.timestamp();
    return result.value_ptr;
}

pub fn getIndex(self: *Self, id: u8) !*IndexRef {
    return try self.acquireIndex(id, false);
}

pub fn createIndex(self: *Self, id: u8) !void {
    const index_ref = try self.acquireIndex(id, true);
    defer self.releaseIndex(index_ref);
}

pub fn deleteIndex(self: *Self, id: u8) !void {
    self.lock.lock();
    defer self.lock.unlock();

    if (self.indexes.getEntry(id)) |entry| {
        entry.value_ptr.index.deinit();
        self.indexes.removeByPtr(entry.key_ptr);
    }

    var sub_dir_name_buf: [max_sub_dir_name_size]u8 = undefined;
    const sub_dir_name = try std.fmt.bufPrint(&sub_dir_name_buf, sub_dir_name_fmt, .{id});

    try self.dir.deleteTree(sub_dir_name);
}
