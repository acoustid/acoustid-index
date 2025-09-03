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
    // Create NATS stream
    try self.createStream(allocator, index_name);
    
    // Publish create operation to NATS (will be handled by consumer)
    // TODO: Publish create index message
    
    // For now, return a dummy response - this will be proper once consumers are working
    return api.CreateIndexResponse{ .version = 1 };
}

pub fn deleteIndex(self: *Self, name: []const u8) !void {
    // Publish delete operation to NATS (will be handled by consumer)
    // TODO: Publish delete index message
    
    // Delete NATS stream
    try self.deleteStream(name);
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
    // Publish to NATS - local application will happen via consumer
    try self.publishUpdate(allocator, index_name, request);
    
    // Return a placeholder response - proper version will come from consumer processing
    return api.UpdateResponse{ .version = 1 };
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

fn getStreamName(allocator: std.mem.Allocator, index_name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(allocator, "fpindex-updates-{s}", .{index_name});
}

fn getSubject(allocator: std.mem.Allocator, index_name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(allocator, "fpindex.updates.{s}", .{index_name});
}

fn createStream(self: *Self, allocator: std.mem.Allocator, index_name: []const u8) !void {
    const stream_name = try getStreamName(allocator, index_name);
    defer allocator.free(stream_name);
    
    const subject = try getSubject(allocator, index_name);
    defer allocator.free(subject);
    
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &[_][]const u8{subject},
        .retention = .limits,
        .max_msgs = 0, // Keep all messages
        .max_age = 0,  // Keep all messages
        .storage = .file,
        .num_replicas = 1, // Start with 1 replica for simplicity
    };
    
    _ = try self.js.addStream(stream_config);
}

fn deleteStream(self: *Self, index_name: []const u8) !void {
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();
    
    const stream_name = try getStreamName(arena.allocator(), index_name);
    
    try self.js.deleteStream(stream_name);
}

fn publishUpdate(self: *Self, allocator: std.mem.Allocator, index_name: []const u8, request: api.UpdateRequest) !void {
    _ = self;
    _ = allocator;
    _ = index_name;
    _ = request;
    // TODO: Implement NATS publish with sequence return
}
