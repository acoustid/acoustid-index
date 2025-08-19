const std = @import("std");

const Self = @This();

allocator: std.mem.Allocator,
entries: std.StringHashMapUnmanaged([]const u8),
owned: bool, // If true, all keys and values are owned and need freeing

/// Initialize empty metadata with given allocator (owned by default)
pub fn init(allocator: std.mem.Allocator) Self {
    return Self{
        .allocator = allocator,
        .entries = .{},
        .owned = true,
    };
}

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

/// Set a key-value pair
pub fn set(self: *Self, key: []const u8, value: []const u8) !void {
    const result = try self.entries.getOrPut(self.allocator, key);
    
    // If updating existing entry, free old values if they were owned
    if (result.found_existing and self.owned) {
        self.allocator.free(result.key_ptr.*);
        self.allocator.free(result.value_ptr.*);
    }
    
    // Set up the new entry based on ownership mode
    if (self.owned) {
        result.key_ptr.* = try self.allocator.dupe(u8, key);
        result.value_ptr.* = try self.allocator.dupe(u8, value);
    } else {
        result.key_ptr.* = key;
        result.value_ptr.* = value;
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

/// Iterator for key-value pairs
pub const Iterator = struct {
    inner: std.StringHashMapUnmanaged([]const u8).Iterator,
    
    pub fn next(self: *Iterator) ?struct { key: []const u8, value: []const u8 } {
        const entry = self.inner.next() orelse return null;
        return .{ .key = entry.key_ptr.*, .value = entry.value_ptr.* };
    }
};

/// Get iterator over key-value pairs
pub fn iterator(self: Self) Iterator {
    return Iterator{ .inner = self.entries.iterator() };
}

/// Ensure capacity for a given number of entries
pub fn ensureTotalCapacity(self: *Self, capacity: u32) !void {
    try self.entries.ensureTotalCapacity(self.allocator, capacity);
}

/// Copy all entries from another metadata instance
pub fn copyFrom(self: *Self, other: Self) !void {
    try self.ensureTotalCapacity(@intCast(other.count()));
    var iter = other.iterator();
    while (iter.next()) |entry| {
        try self.set(entry.key, entry.value);
    }
}

/// Merge entries from another metadata instance, with this instance taking precedence for conflicts
pub fn mergeFrom(self: *Self, other: Self) !void {
    var iter = other.iterator();
    while (iter.next()) |entry| {
        if (self.get(entry.key) == null) {
            try self.set(entry.key, entry.value);
        }
    }
}

/// JSON serialization
pub fn jsonStringify(self: Self, jws: anytype) !void {
    try jws.beginObject();
    var iter = self.iterator();
    while (iter.next()) |entry| {
        try jws.objectField(entry.key);
        try jws.write(entry.value);
    }
    try jws.endObject();
}

/// MessagePack serialization
pub fn msgpackWrite(self: Self, packer: anytype) !void {
    try packer.writeMapHeader(self.count());
    var iter = self.iterator();
    while (iter.next()) |entry| {
        try packer.write(entry.key);
        try packer.write(entry.value);
    }
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

/// Load from msgpack into this metadata instance (ownership based on metadata instance)
pub fn loadFromMsgpack(self: *Self, reader: anytype, allocator: std.mem.Allocator) !void {
    // First load into a temporary unmanaged map
    var temp_map: std.StringHashMapUnmanaged([]const u8) = .{};
    defer temp_map.deinit(allocator);
    
    const msgpack = @import("msgpack");
    try msgpack.unpackMapInto(reader, allocator, &temp_map);
    
    // Now transfer to our metadata
    try self.ensureTotalCapacity(@intCast(temp_map.count()));
    var iter = temp_map.iterator();
    while (iter.next()) |entry| {
        try self.set(entry.key_ptr.*, entry.value_ptr.*);
    }
}