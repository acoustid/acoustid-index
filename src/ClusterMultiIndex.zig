const std = @import("std");
const nats = @import("nats");
const msgpack = @import("msgpack");

const MultiIndex = @import("MultiIndex.zig");
const Index = @import("Index.zig");
const api = @import("api.zig");

const Self = @This();

// Message queue type
const MessageQueue = std.fifo.LinearFifo(*nats.JetStreamMessage, .Dynamic);

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

// Message processing infrastructure
processor_thread: ?std.Thread = null,
stopping: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

// Per-index message queues and subscriptions
queues: std.StringHashMap(MessageQueue),
queues_mutex: std.Thread.Mutex = .{},
processor_cond: std.Thread.Condition = .{},
subscriptions: std.StringHashMap(*nats.Subscription),

pub fn init(allocator: std.mem.Allocator, nats_connection: *nats.Connection, local_indexes: *MultiIndex) Self {
    const js = nats_connection.jetstream(.{});
    return .{
        .allocator = allocator,
        .nats_connection = nats_connection,
        .js = js,
        .local_indexes = local_indexes,
        .queues = std.StringHashMap(MessageQueue).init(allocator),
        .subscriptions = std.StringHashMap(*nats.Subscription).init(allocator),
    };
}

pub fn deinit(self: *Self) void {
    // Ensure thread is stopped
    self.stop();
    
    // Clean up queues and subscriptions
    var queue_it = self.queues.iterator();
    while (queue_it.next()) |entry| {
        entry.value_ptr.deinit();
    }
    self.queues.deinit();
    self.subscriptions.deinit();
}

pub fn start(self: *Self) !void {
    if (self.processor_thread != null) {
        return; // Already started
    }
    
    // Start message processor thread  
    self.processor_thread = try std.Thread.spawn(.{}, processorThreadFn, .{self});
    
    // For now, start consuming from hardcoded "test" index
    try self.startConsumingIndex("test");
}

pub fn stop(self: *Self) void {
    // Signal stop
    self.stopping.store(true, .monotonic);
    
    // Wake up processor thread
    self.processor_cond.signal();
    
    // Wait for thread to finish
    if (self.processor_thread) |thread| {
        thread.join();
        self.processor_thread = null;
    }
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

fn processorThreadFn(self: *Self) void {
    const log = std.log.scoped(.processor);
    log.info("Message processor thread started", .{});
    
    while (!self.stopping.load(.monotonic)) {
        // Lock queues and wait for messages
        self.queues_mutex.lock();
        defer self.queues_mutex.unlock();
        
        // Process all queued messages
        var has_messages = false;
        var queue_it = self.queues.iterator();
        while (queue_it.next()) |entry| {
            const index_name = entry.key_ptr.*;
            const queue = entry.value_ptr;
            
            while (queue.readItem()) |msg| {
                has_messages = true;
                self.processMessage(index_name, msg) catch |err| {
                    log.err("Failed to process message for index '{s}': {}", .{ index_name, err });
                };
            }
        }
        
        // If no messages, wait for signal
        if (!has_messages and !self.stopping.load(.monotonic)) {
            self.processor_cond.wait(&self.queues_mutex);
        }
    }
    
    log.info("Message processor thread stopped", .{});
}

fn startConsumingIndex(self: *Self, index_name: []const u8) !void {
    const log = std.log.scoped(.consumer);
    
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();
    
    const subject = try getSubject(arena.allocator(), index_name);
    
    // Create queue for this index
    self.queues_mutex.lock();
    defer self.queues_mutex.unlock();
    
    const queue = MessageQueue.init(self.allocator);
    try self.queues.put(try self.allocator.dupe(u8, index_name), queue);
    
    // Subscribe with callback (this is a placeholder - need to implement with proper NATS API)
    log.info("Started consuming index '{s}' on subject '{s}'", .{ index_name, subject });
}

fn messageCallback(self: *Self, index_name: []const u8, msg: *nats.JetStreamMessage) void {
    // Queue the message
    self.queues_mutex.lock();
    defer self.queues_mutex.unlock();
    
    if (self.queues.getPtr(index_name)) |queue| {
        queue.writeItem(msg) catch {
            // Queue full - could log warning or implement backpressure
        };
        self.processor_cond.signal();
    }
}

fn processMessage(self: *Self, index_name: []const u8, msg: *nats.JetStreamMessage) !void {
    const log = std.log.scoped(.processor);
    
    // Decode the operation
    const operation = msgpack.decodeFromSliceLeaky(Operation, self.allocator, msg.msg.data) catch |err| {
        log.err("Failed to decode operation for index '{s}': {}", .{ index_name, err });
        try msg.nak();
        return err;
    };
    defer if (@hasDecl(Operation, "deinit")) operation.deinit();
    
    // Process the operation
    self.processOperation(index_name, operation) catch |err| {
        log.err("Failed to process operation for index '{s}': {}", .{ index_name, err });
        try msg.nak();
        return err;
    };
    
    // Acknowledge successful processing
    try msg.ack();
    log.debug("Processed operation for index '{s}' at sequence {?}", .{ index_name, msg.metadata.sequence.stream });
}

fn processOperation(self: *Self, index_name: []const u8, operation: Operation) !void {
    const log = std.log.scoped(.consumer);
    
    switch (operation) {
        .create => {
            log.info("Creating index '{s}' locally", .{index_name});
            _ = try self.local_indexes.createIndex(self.allocator, index_name);
        },
        .delete => {
            log.info("Deleting index '{s}' locally", .{index_name});
            try self.local_indexes.deleteIndex(index_name);
        },
        .update => |request| {
            log.debug("Applying update to index '{s}' locally", .{index_name});
            _ = try self.local_indexes.update(self.allocator, index_name, request);
        },
    }
}
