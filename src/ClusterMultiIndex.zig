const std = @import("std");
const nats = @import("nats");

const MultiIndex = @import("MultiIndex.zig");
const Index = @import("Index.zig");
const api = @import("api.zig");

const Self = @This();

allocator: std.mem.Allocator,
nats_connection: *nats.Connection,
js: nats.JetStream,
local_indexes: *MultiIndex,

pub fn init(allocator: std.mem.Allocator, nats_connection: *nats.Connection, local_indexes: *MultiIndex) Self {
    const js = nats_connection.jetstream(.{});
    return .{
        .allocator = allocator,
        .nats_connection = nats_connection,
        .js = js,
        .local_indexes = local_indexes,
    };
}

pub fn deinit(self: *Self) void {
    _ = self;
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
