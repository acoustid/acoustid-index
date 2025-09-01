const std = @import("std");
const nats = @import("nats");

const MultiIndex = @import("MultiIndex.zig");
const Index = @import("Index.zig");
const api = @import("api.zig");

const Self = @This();

allocator: std.mem.Allocator,
nats_connection: *nats.Connection,
local_indexes: *MultiIndex,

pub fn init(allocator: std.mem.Allocator, nats_connection: *nats.Connection, local_indexes: *MultiIndex) Self {
    return .{
        .allocator = allocator,
        .nats_connection = nats_connection,
        .local_indexes = local_indexes,
    };
}

pub fn deinit(self: *Self) void {
    _ = self;
}

pub fn getOrCreateIndex(self: *Self, name: []const u8, create: bool) !*Index {
    return self.local_indexes.getOrCreateIndex(name, create);
}

pub fn getIndex(self: *Self, name: []const u8) !*Index {
    return self.local_indexes.getIndex(name);
}

pub fn createIndex(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
) !api.CreateIndexResponse {
    return self.local_indexes.createIndex(allocator, index_name);
}

pub fn deleteIndex(self: *Self, name: []const u8) !void {
    return self.local_indexes.deleteIndex(name);
}

pub fn releaseIndex(self: *Self, index: *Index) void {
    self.local_indexes.releaseIndex(index);
}

pub fn search(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
    request: api.SearchRequest,
) !api.SearchResponse {
    return self.local_indexes.search(allocator, index_name, request);
}

pub fn update(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
    request: api.UpdateRequest,
) !api.UpdateResponse {
    return self.local_indexes.update(allocator, index_name, request);
}

pub fn getIndexInfo(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
) !api.GetIndexInfoResponse {
    return self.local_indexes.getIndexInfo(allocator, index_name);
}

pub fn checkIndexExists(
    self: *Self,
    index_name: []const u8,
) !void {
    return self.local_indexes.checkIndexExists(index_name);
}

pub fn getFingerprintInfo(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
    fingerprint_id: u32,
) !api.GetFingerprintInfoResponse {
    return self.local_indexes.getFingerprintInfo(allocator, index_name, fingerprint_id);
}

pub fn checkFingerprintExists(
    self: *Self,
    index_name: []const u8,
    fingerprint_id: u32,
) !void {
    return self.local_indexes.checkFingerprintExists(index_name, fingerprint_id);
}