const std = @import("std");

const Self = @This();

allocator: std.mem.Allocator,
entries: std.StringHashMapUnmanaged([]const u8),
owned: bool, // If true, all keys and values are owned and need freeing

/// Initialize empty owned metadata
pub fn initOwned(allocator: std.mem.Allocator) Self {
    return Self{
        .allocator = allocator,
        .entries = .{},
        .owned = true,
    };
}

/// Initialize empty borrowed metadata
pub fn initBorrowed(allocator: std.mem.Allocator) Self {
    return Self{
        .allocator = allocator,
        .entries = .{},
        .owned = false,
    };
}

/// Deinitialize and free all owned keys and values
pub fn deinit(self: *Self) void {
    if (self.owned) {
        var iter = self.entries.iterator();
        while (iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
    }
    self.entries.deinit(self.allocator);
}

/// Set a borrowed key-value pair
fn setBorrowed(self: *Self, key: []const u8, value: []const u8) !void {
    const result = try self.entries.getOrPut(self.allocator, key);
    if (!result.found_existing) {
        result.key_ptr.* = key;
    }

    result.value_ptr.* = value;
}

/// Set an owned key-value pair
fn setOwned(self: *Self, key: []const u8, value: []const u8) !void {
    const owned_value = try self.allocator.dupe(u8, value);
    errdefer self.allocator.free(owned_value);

    const result = try self.entries.getOrPut(self.allocator, key);
    if (!result.found_existing) {
        errdefer self.entries.removeByPtr(result.key_ptr);
        result.key_ptr.* = try self.allocator.dupe(u8, key);
    }

    result.value_ptr.* = owned_value;
}

/// Set a key-value pair
pub fn set(self: *Self, key: []const u8, value: []const u8) !void {
    if (self.owned) {
        return self.setOwned(key, value);
    } else {
        return self.setBorrowed(key, value);
    }
}

/// Get value for a key, returns null if not found
pub fn get(self: Self, key: []const u8) ?[]const u8 {
    return self.entries.get(key);
}

/// Remove a key-value pair, freeing owned memory
pub fn remove(self: *Self, key: []const u8) bool {
    const kv = self.entries.fetchRemove(key) orelse return false;
    if (self.owned) {
        self.allocator.free(kv.key);
        self.allocator.free(kv.value);
    }
    return true;
}

/// Get count of entries
pub fn count(self: Self) usize {
    return self.entries.count();
}

/// Update entries from another metadata instance, replacing all existing entries
pub fn update(self: *Self, other: Self) !void {
    var iter = other.entries.iterator();
    while (iter.next()) |entry| {
        try self.set(entry.key_ptr.*, entry.value_ptr.*);
    }
}

/// JSON serialization
pub fn jsonStringify(self: Self, jws: anytype) !void {
    try jws.beginObject();
    var iter = self.entries.iterator();
    while (iter.next()) |entry| {
        try jws.objectField(entry.key_ptr.*);
        try jws.write(entry.value_ptr.*);
    }
    try jws.endObject();
}

/// JSON parsing
pub fn jsonParse(allocator: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !Self {
    var parsed = try std.json.ArrayHashMap([]const u8).jsonParse(allocator, source, options);
    errdefer parsed.deinit(allocator);

    var self = Self.initBorrowed(allocator);
    errdefer self.deinit();

    var iter = parsed.map.iterator();
    while (iter.next()) |entry| {
        try self.set(entry.key_ptr.*, entry.value_ptr.*);
    }

    return self;
}

/// MessagePack serialization
pub fn msgpackWrite(self: Self, packer: anytype) !void {
    try packer.writeMapHeader(self.count());
    var iter = self.entries.iterator();
    while (iter.next()) |entry| {
        try packer.write(entry.key_ptr.*);
        try packer.write(entry.value_ptr.*);
    }
}

/// MessagePack parsing
pub fn msgpackRead(unpacker: anytype) !Self {
    var self = Self.initOwned(unpacker.allocator);
    errdefer self.deinit();

    try unpacker.readMapInto(&self.entries);

    return self;
}

/// Create metadata from unmanaged hashmap, taking ownership of all strings
pub fn fromUnmanagedOwned(allocator: std.mem.Allocator, hashmap: std.StringHashMapUnmanaged([]const u8)) !Self {
    var metadata = initOwned(allocator);
    errdefer metadata.deinit();

    try metadata.ensureTotalCapacity(@intCast(hashmap.count()));
    var iter = hashmap.iterator();
    while (iter.next()) |entry| {
        try metadata.set(entry.key_ptr.*, entry.value_ptr.*);
    }

    return metadata;
}

/// Create metadata from unmanaged hashmap, borrowing all strings
pub fn fromUnmanagedBorrowed(allocator: std.mem.Allocator, hashmap: std.StringHashMapUnmanaged([]const u8)) !Self {
    var metadata = initBorrowed(allocator);
    errdefer metadata.deinit();

    try metadata.ensureTotalCapacity(@intCast(hashmap.count()));
    var iter = hashmap.iterator();
    while (iter.next()) |entry| {
        try metadata.set(entry.key_ptr.*, entry.value_ptr.*);
    }

    return metadata;
}

/// Convert to unmanaged hashmap (returns borrowed references to internal strings)
pub fn toUnmanaged(self: Self, allocator: std.mem.Allocator) !std.StringHashMapUnmanaged([]const u8) {
    var hashmap: std.StringHashMapUnmanaged([]const u8) = .{};
    errdefer hashmap.deinit(allocator);

    try hashmap.ensureTotalCapacity(allocator, @intCast(self.count()));
    var iter = self.entries.iterator();
    while (iter.next()) |entry| {
        hashmap.putAssumeCapacity(entry.key_ptr.*, entry.value_ptr.*);
    }

    return hashmap;
}

/// Clear all entries, retaining capacity
pub fn clearRetainingCapacity(self: *Self) void {
    if (self.owned) {
        var iter = self.entries.iterator();
        while (iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
    }
    self.entries.clearRetainingCapacity();
}
