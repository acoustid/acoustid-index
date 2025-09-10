const std = @import("std");
const nats = @import("nats");
const msgpack = @import("msgpack");
const inbox = @import("nats").inbox;

const MultiIndex = @import("MultiIndex.zig");
const Index = @import("Index.zig");
const api = @import("api.zig");

const log = std.log.scoped(.fpindex);

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
    create_index: CreateIndexOp,
    delete_index: DeleteIndexOp,
    update: api.UpdateRequest,

    pub fn msgpackFormat() msgpack.UnionFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

const STATUS_STREAM_NAME = "fpindex-status";
const STATUS_SUBJECT_PREFIX = "fpindex.status.";
const STATUS_SUBJECT_WILDCARD = STATUS_SUBJECT_PREFIX ++ "*";

const UPDATES_STREAM_NAME_PREFIX = "fpindex-updates-";
const UPDATES_SUBJECT_PREFIX = "fpindex.updates.";
const UPDATES_SUBJECT_WILDCARD = UPDATES_SUBJECT_PREFIX ++ "*";

allocator: std.mem.Allocator,
indexes: *MultiIndex,
js: nats.JetStream,

status_sub: ?*nats.JetStreamSubscription = null,

pub fn init(allocator: std.mem.Allocator, nc: *nats.Connection, indexes: *MultiIndex) Self {
    const js = nc.jetstream(.{});
    return .{
        .allocator = allocator,
        .indexes = indexes,
        .js = js,
    };
}

pub fn deinit(self: *Self) void {
    self.stop();
}

pub fn start(self: *Self) !void {
    try self.createStatusStream();
    try self.subscribeToStatusStream();
}

pub fn stop(self: *Self) void {
    if (self.status_sub) |sub| {
        sub.deinit();
        self.status_sub = null;
    }
}

pub fn createIndex(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
) !api.CreateIndexResponse {
    _ = allocator;

    var stream_info = try self.createStream(index_name);
    defer stream_info.deinit();

    var last_seq = stream_info.value.state.last_seq;
    if (last_seq == 0) {
        const op = Operation{ .create_index = CreateIndexOp{} };
        last_seq = try self.publishOperation(index_name, op, last_seq); // TODO catch unexpected seq
    }

    return api.CreateIndexResponse{ .version = last_seq };
}

pub fn deleteIndex(self: *Self, name: []const u8) !void {
    // Use arena allocator for temporary allocation
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();

    // Publish delete operation to NATS (will be handled by consumer)
    _ = try self.publishOperation(name, Operation{ .delete_index = DeleteIndexOp{} }, null);

    // Stop consumer subscription for this index
    try self.stopIndexUpdater(name);

    // Delete NATS stream after stopping consumer
    try self.deleteStream(name);
}

pub fn search(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
    request: api.SearchRequest,
) !api.SearchResponse {
    return self.indexes.search(allocator, index_name, request);
}

pub fn update(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
    request: api.UpdateRequest,
) !api.UpdateResponse {
    _ = allocator;

    // Publish to NATS - local application will happen via consumer
    const seq = try self.publishOperation(index_name, Operation{ .update = request }, null);

    // Return response with the NATS sequence as version
    return api.UpdateResponse{ .version = seq };
}

pub fn getIndexInfo(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
) !api.GetIndexInfoResponse {
    return self.indexes.getIndexInfo(allocator, index_name);
}

pub fn checkIndexExists(
    self: *Self,
    index_name: []const u8,
) !void {
    return self.indexes.checkIndexExists(index_name);
}

pub fn getFingerprintInfo(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
    fingerprint_id: u32,
) !api.GetFingerprintInfoResponse {
    return self.indexes.getFingerprintInfo(allocator, index_name, fingerprint_id);
}

pub fn checkFingerprintExists(
    self: *Self,
    index_name: []const u8,
    fingerprint_id: u32,
) !void {
    return self.indexes.checkFingerprintExists(index_name, fingerprint_id);
}

fn getUpdatesStreamName(allocator: std.mem.Allocator, index_name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(allocator, "fpindex-updates-{s}", .{index_name});
}

fn getUpdatesSubject(allocator: std.mem.Allocator, index_name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(allocator, "fpindex.updates.{s}", .{index_name});
}

fn createStream(self: *Self, index_name: []const u8) !nats.Result(nats.StreamInfo) {
    const stream_name = try getUpdatesStreamName(self.allocator, index_name);
    defer self.allocator.free(stream_name);

    const subject = try getUpdatesSubject(self.allocator, index_name);
    defer self.allocator.free(subject);

    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &[_][]const u8{subject},
        .retention = .limits,
        .max_msgs = 0, // Keep all messages
        .max_age = 0, // Keep all messages
        .storage = .file,
        .num_replicas = 1, // Start with 1 replica for simplicity
    };

    log.debug("Creating stream '{s}'", .{stream_name});

    const result = self.js.addStream(stream_config) catch |err| {
        log.info("Failed to create stream '{s}': {}", .{ stream_name, err });
        return err;
    };

    log.info("Created stream '{s}': {any}", .{ stream_name, result.value });

    return result;
}

fn deleteStream(self: *Self, index_name: []const u8) !void {
    const stream_name = try getUpdatesStreamName(self.allocator, index_name);
    defer self.allocator.free(stream_name);

    log.debug("Deleting stream '{s}'", .{stream_name});

    try self.js.deleteStream(stream_name);

    log.info("Deleted stream '{s}'", .{stream_name});
}

fn publishOperation(self: *Self, index_name: []const u8, operation: Operation, expected_last_seq: ?u64) !u64 {
    const subject = try getUpdatesSubject(self.allocator, index_name);
    defer self.allocator.free(subject);

    // Encode the operation as msgpack to byte array
    var buf = std.ArrayList(u8).init(self.allocator);
    defer buf.deinit();
    try msgpack.encode(operation, buf.writer());

    // Publish to NATS JetStream with optional sequence check
    const result = try self.js.publish(subject, buf.items, .{
        .expected_last_seq = expected_last_seq,
    });
    defer result.deinit();

    return result.value.seq;
}

fn startIndexUpdater(self: *Self, index_name: []const u8) !void {
    _ = self;
    _ = index_name;
}

fn stopIndexUpdater(self: *Self, index_name: []const u8) !void {
    _ = self;
    _ = index_name;
}

fn createStatusStream(self: *Self) !void {
    const stream_config = nats.StreamConfig{
        .name = STATUS_STREAM_NAME,
        .subjects = &[_][]const u8{STATUS_SUBJECT_WILDCARD},
        .max_msgs_per_subject = 3, // allow short history
        .allow_direct = true, // allow direct get
        .storage = .file,
    };

    const stream_info = self.js.addStream(stream_config) catch |err| {
        log.err("Failed to create status stream: {}", .{err});
        return err;
    };
    defer stream_info.deinit();
}

fn handleStatusUpdate(self: *Self, msg: *nats.JetStreamMessage) !void {
    _ = self;
    log.info("Received status update '{s}'", .{msg.msg.subject});
}

fn onStatusUpdate(msg: *nats.JetStreamMessage, self: *Self) !void {
    defer msg.deinit();
    self.handleStatusUpdate(msg) catch |err| {
        try msg.nak();
        log.err("Failed to handle status update: {}", .{err});
        return;
    };
    try msg.ack();
}

fn subscribeToStatusStream(self: *Self) !void {
    const consumer_config = nats.ConsumerConfig{
        .name = null,
        .deliver_group = null,
        .deliver_policy = .all,
    };
    self.status_sub = try self.js.subscribe(STATUS_STREAM_NAME, consumer_config, onStatusUpdate, .{self});
}
