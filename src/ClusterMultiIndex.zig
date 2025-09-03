const std = @import("std");
const nats = @import("nats");
const msgpack = @import("msgpack");

const MultiIndex = @import("MultiIndex.zig");
const Index = @import("Index.zig");
const api = @import("api.zig");

const Self = @This();

// Operation types for NATS messages
const CreateIndexOp = struct {
    // Empty for now - just indicates index should be created
    
    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

const DeleteIndexOp = struct {
    // Empty for now - just indicates index should be deleted
    
    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

const Operation = union(enum) {
    create: CreateIndexOp,
    delete: DeleteIndexOp,
    update: api.UpdateRequest,
    
    pub fn msgpackFormat() msgpack.UnionFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

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
    const seq = try self.publishOperation(allocator, index_name, Operation{ .create = CreateIndexOp{} });
    
    // Return response with NATS sequence as version
    return api.CreateIndexResponse{ .version = seq };
}

pub fn deleteIndex(self: *Self, name: []const u8) !void {
    // Use arena allocator for temporary allocation
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();
    
    // Publish delete operation to NATS (will be handled by consumer)
    _ = try self.publishOperation(arena.allocator(), name, Operation{ .delete = DeleteIndexOp{} });
    
    // Delete NATS stream after publishing delete operation
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
    const seq = try self.publishOperation(allocator, index_name, Operation{ .update = request });
    
    // Return response with the NATS sequence as version
    return api.UpdateResponse{ .version = seq };
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

fn publishOperation(self: *Self, allocator: std.mem.Allocator, index_name: []const u8, operation: Operation) !u64 {
    const subject = try getSubject(allocator, index_name);
    defer allocator.free(subject);
    
    // Encode the operation as msgpack to byte array
    var buf = std.ArrayList(u8).init(allocator);
    defer buf.deinit();
    try msgpack.encode(operation, buf.writer());
    
    // Publish to NATS JetStream
    const result = try self.js.publish(subject, buf.items, .{});
    defer result.deinit();
    
    return result.value.seq;
}
