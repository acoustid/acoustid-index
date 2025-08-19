const std = @import("std");

const Self = @This();

/// Ownership mode for keys and values
pub const Ownership = enum {
    owned,     // Keys/values are allocated and need to be freed
    borrowed,  // Keys/values are borrowed references, don't free
};

/// Internal entry to track ownership per key-value pair
const Entry = struct {
    key: []const u8,
    value: []const u8,
    key_owned: bool,
    value_owned: bool,
};

allocator: std.mem.Allocator,
entries: std.StringHashMapUnmanaged(Entry),

/// Initialize empty metadata with given allocator
pub fn init(allocator: std.mem.Allocator) Self {
    return Self{
        .allocator = allocator,
        .entries = .{},
    };
}

/// Deinitialize and free all owned keys and values
pub fn deinit(self: *Self) void {
    var iter = self.entries.iterator();
    while (iter.next()) |entry| {
        if (entry.value_ptr.key_owned) {
            self.allocator.free(entry.value_ptr.key);
        }
        if (entry.value_ptr.value_owned) {
            self.allocator.free(entry.value_ptr.value);
        }
    }
    self.entries.deinit(self.allocator);
}

/// Set a key-value pair with specified ownership
pub fn set(self: *Self, key: []const u8, value: []const u8, key_ownership: Ownership, value_ownership: Ownership) !void {
    const result = try self.entries.getOrPut(self.allocator, key);
    
    // If updating existing entry, free old values if they were owned
    if (result.found_existing) {
        if (result.value_ptr.key_owned) {
            self.allocator.free(result.value_ptr.key);
        }
        if (result.value_ptr.value_owned) {
            self.allocator.free(result.value_ptr.value);
        }
    }
    
    // Set up the new entry
    const key_owned = key_ownership == .owned;
    const value_owned = value_ownership == .owned;
    
    result.value_ptr.* = Entry{
        .key = if (key_owned) try self.allocator.dupe(u8, key) else key,
        .value = if (value_owned) try self.allocator.dupe(u8, value) else value,
        .key_owned = key_owned,
        .value_owned = value_owned,
    };
    
    // Update the hashmap's key pointer to our stored key
    result.key_ptr.* = result.value_ptr.key;
}

/// Set a key-value pair, taking ownership of both (convenience method)
pub fn setOwned(self: *Self, key: []const u8, value: []const u8) !void {
    try self.set(key, value, .owned, .owned);
}

/// Set a key-value pair without taking ownership (convenience method)
pub fn setBorrowed(self: *Self, key: []const u8, value: []const u8) !void {
    try self.set(key, value, .borrowed, .borrowed);
}

/// Get value for a key, returns null if not found
pub fn get(self: Self, key: []const u8) ?[]const u8 {
    const entry = self.entries.get(key);
    return if (entry) |e| e.value else null;
}

/// Remove a key-value pair, freeing owned memory
pub fn remove(self: *Self, key: []const u8) bool {
    const kv = self.entries.fetchRemove(key) orelse return false;
    if (kv.value.key_owned) {
        self.allocator.free(kv.value.key);
    }
    if (kv.value.value_owned) {
        self.allocator.free(kv.value.value);
    }
    return true;
}

/// Get count of entries
pub fn count(self: Self) usize {
    return self.entries.count();
}

/// Iterator for key-value pairs
pub const Iterator = struct {
    inner: std.StringHashMapUnmanaged(Entry).Iterator,
    
    pub fn next(self: *Iterator) ?struct { key: []const u8, value: []const u8 } {
        const entry = self.inner.next() orelse return null;
        return .{ .key = entry.value_ptr.key, .value = entry.value_ptr.value };
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

/// Copy all entries from another metadata instance, taking ownership
pub fn copyFrom(self: *Self, other: Self) !void {
    try self.ensureTotalCapacity(@intCast(other.count()));
    var iter = other.iterator();
    while (iter.next()) |entry| {
        try self.setOwned(entry.key, entry.value);
    }
}

/// Merge entries from another metadata instance, with this instance taking precedence for conflicts
pub fn mergeFrom(self: *Self, other: Self) !void {
    var iter = other.iterator();
    while (iter.next()) |entry| {
        if (self.get(entry.key) == null) {
            try self.setOwned(entry.key, entry.value);
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
    var metadata = init(allocator);
    errdefer metadata.deinit();
    
    try metadata.ensureTotalCapacity(@intCast(hashmap.count()));
    var iter = hashmap.iterator();
    while (iter.next()) |entry| {
        try metadata.setOwned(entry.key_ptr.*, entry.value_ptr.*);
    }
    
    return metadata;
}

/// Create metadata from unmanaged hashmap, borrowing all strings
pub fn fromUnmanagedBorrowed(allocator: std.mem.Allocator, hashmap: std.StringHashMapUnmanaged([]const u8)) !Self {
    var metadata = init(allocator);
    errdefer metadata.deinit();
    
    try metadata.ensureTotalCapacity(@intCast(hashmap.count()));
    var iter = hashmap.iterator();
    while (iter.next()) |entry| {
        try metadata.setBorrowed(entry.key_ptr.*, entry.value_ptr.*);
    }
    
    return metadata;
}

/// Convert to unmanaged hashmap (returns borrowed references to internal strings)
pub fn toUnmanaged(self: Self, allocator: std.mem.Allocator) !std.StringHashMapUnmanaged([]const u8) {
    var hashmap: std.StringHashMapUnmanaged([]const u8) = .{};
    errdefer hashmap.deinit(allocator);
    
    try hashmap.ensureTotalCapacity(allocator, @intCast(self.count()));
    var iter = self.iterator();
    while (iter.next()) |entry| {
        hashmap.putAssumeCapacity(entry.key, entry.value);
    }
    
    return hashmap;
}

/// Clear all entries, retaining capacity
pub fn clearRetainingCapacity(self: *Self) void {
    var iter = self.entries.iterator();
    while (iter.next()) |entry| {
        if (entry.value_ptr.key_owned) {
            self.allocator.free(entry.value_ptr.key);
        }
        if (entry.value_ptr.value_owned) {
            self.allocator.free(entry.value_ptr.value);
        }
    }
    self.entries.clearRetainingCapacity();
}

/// Load from msgpack into this metadata instance, taking ownership of all strings
pub fn loadFromMsgpackOwned(self: *Self, reader: anytype, allocator: std.mem.Allocator) !void {
    // First load into a temporary unmanaged map
    var temp_map: std.StringHashMapUnmanaged([]const u8) = .{};
    defer temp_map.deinit(allocator);
    
    const msgpack = @import("msgpack");
    try msgpack.unpackMapInto(reader, allocator, &temp_map);
    
    // Now transfer to our metadata with ownership
    try self.ensureTotalCapacity(@intCast(temp_map.count()));
    var iter = temp_map.iterator();
    while (iter.next()) |entry| {
        try self.setOwned(entry.key_ptr.*, entry.value_ptr.*);
    }
}