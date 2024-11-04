const std = @import("std");
const assert = std.debug.assert;

const Index = @import("Index.zig");

const Self = @This();

const IndexData = struct {
    index: Index,
    dir: std.fs.Dir,
    references: usize = 0,
    last_used_at: i64 = std.math.minInt(i64),
};

lock: std.Thread.Mutex = .{},
allocator: std.mem.Allocator,
dir: std.fs.Dir,
indexes: std.AutoHashMap(u8, IndexData),

pub fn init(allocator: std.mem.Allocator, dir: std.fs.Dir) Self {
    return .{
        .allocator = allocator,
        .dir = dir,
        .indexes = std.AutoHashMap(u8, IndexData).init(allocator),
    };
}

pub fn deinit(self: *Self) void {
    var iter = self.indexes.iterator();
    while (iter.next()) |entry| {
        entry.value_ptr.index.deinit();
        entry.value_ptr.dir.close();
    }
    self.indexes.deinit();
    self.* = undefined;
}

pub fn releaseIndex(self: *Self, index_data: *IndexData) void {
    self.lock.lock();
    defer self.lock.unlock();

    assert(index_data.references > 0);
    index_data.references -= 1;
    index_data.last_used_at = std.time.timestamp();
}

pub fn getIndex(self: *Self, id: u8) !*IndexData {
    self.lock.lock();
    defer self.lock.unlock();

    var result = try self.indexes.getOrPut(id);
    if (result.found_existing) {
        result.value_ptr.references += 1;
        result.value_ptr.last_used_at = std.time.timestamp();
        return result.value_ptr;
    }

    errdefer self.indexes.removeByPtr(result.key_ptr);

    var file_name_buf: [8]u8 = undefined;
    const file_name = try std.fmt.bufPrint(&file_name_buf, "{x:0>2}", .{id});

    result.value_ptr.dir = try self.dir.makeOpenPath(file_name, .{ .iterate = true });
    errdefer result.value_ptr.dir.close();

    result.value_ptr.index = try Index.init(self.allocator, result.value_ptr.dir, .{ .create = true });
    errdefer result.value_ptr.index.deinit();

    try result.value_ptr.index.open();

    result.value_ptr.references += 1;
    result.value_ptr.last_used_at = std.time.timestamp();
    return result.value_ptr;
}
