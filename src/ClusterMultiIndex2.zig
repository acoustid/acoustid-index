const std = @import("std");
const log = std.log.scoped(.cluster_multi_index2);
const nats = @import("nats");
const msgpack = @import("msgpack");

const MultiIndex = @import("MultiIndex.zig");
const Index = @import("Index.zig");
const api = @import("api.zig");
const Scheduler = @import("utils/Scheduler.zig");

const Self = @This();

// Index states as defined in the specification
pub const IndexState = enum {
    creating, // stream does not exist yet, reads/writes not allowed
    created,  // stream exists, reads/writes allowed  
    deleting, // stream still exists, reads/writes disallowed, waiting for consumers to stop
    deleted,  // stream does not exist, safe to recreate index
};

// MessagePack-encoded status message
pub const IndexStatus = struct {
    state: IndexState,
    version: u64,
    timestamp: i64,
    
    pub fn encode(self: IndexStatus, allocator: std.mem.Allocator) ![]u8 {
        var buffer = std.ArrayList(u8).init(allocator);
        defer buffer.deinit();
        try msgpack.pack(buffer.writer(), self);
        return try allocator.dupe(u8, buffer.items);
    }
    
    pub fn decode(data: []const u8, allocator: std.mem.Allocator) !IndexStatus {
        var stream = std.io.fixedBufferStream(data);
        return try msgpack.unpack(IndexStatus, allocator, stream.reader());
    }
};

// Update message types for the oplog
pub const UpdateMessage = union(enum) {
    create_index: struct {
        index_name: []const u8,
    },
    delete_index: struct {
        index_name: []const u8,
    },
    fingerprint_update: struct {
        changes: []const @import("change.zig").Change,
        metadata: []const u8,
        expected_version: u64,
    },
    
    pub fn encode(self: UpdateMessage, allocator: std.mem.Allocator) ![]u8 {
        var buffer = std.ArrayList(u8).init(allocator);
        defer buffer.deinit();
        try msgpack.pack(buffer.writer(), self);
        return try allocator.dupe(u8, buffer.items);
    }
    
    pub fn decode(data: []const u8, allocator: std.mem.Allocator) !UpdateMessage {
        var stream = std.io.fixedBufferStream(data);
        return try msgpack.unpack(UpdateMessage, allocator, stream.reader());
    }
};

// Per-index consumer state
const IndexConsumer = struct {
    allocator: std.mem.Allocator,
    index_name: []const u8,
    consumer_name: []const u8,
    subscription: ?*nats.PullSubscription = null,
    last_processed_seq: u64 = 0,
    is_running: bool = false,
    thread: ?std.Thread = null,
    
    pub fn deinit(self: *IndexConsumer) void {
        self.stop();
        self.allocator.free(self.index_name);
        self.allocator.free(self.consumer_name);
    }
    
    pub fn stop(self: *IndexConsumer) void {
        self.is_running = false;
        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
        if (self.subscription) |sub| {
            sub.deinit();
            self.subscription = null;
        }
    }
};

// Main clustered multi-index structure
allocator: std.mem.Allocator,
scheduler: *Scheduler,
nats_connection: *nats.Connection,
jetstream: nats.JetStream,
local_indexes: *MultiIndex,

// Status stream management
status_subscription: ?*nats.Subscription = null,

// Per-index consumers
consumers_mutex: std.Thread.Mutex = .{},
consumers: std.StringHashMap(*IndexConsumer),

// Cleanup process
cleanup_task: ?*Scheduler.Task = null,
cleanup_running: bool = false,

// Stream names (computed from index names)
const STATUS_STREAM_NAME = "fpindex-status";

fn getUpdatesStreamName(allocator: std.mem.Allocator, index_name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, "fpindex-updates-{s}", .{index_name});
}

fn getUpdatesSubject(allocator: std.mem.Allocator, index_name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, "fpindex.updates.{s}", .{index_name});
}

fn getStatusSubject(allocator: std.mem.Allocator, index_name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, "fpindex.status.{s}", .{index_name});
}

fn getConsumerName(allocator: std.mem.Allocator, index_name: []const u8) ![]u8 {
    // Include hostname/process ID for uniqueness across instances
    var hostname_buf: [256]u8 = undefined;
    const hostname = std.os.gethostname(&hostname_buf) catch "unknown";
    const pid = std.os.linux.getpid();
    return try std.fmt.allocPrint(allocator, "fpindex-consumer-{s}-{s}-{d}", .{ index_name, hostname, pid });
}

pub fn init(allocator: std.mem.Allocator, scheduler: *Scheduler, nats_connection: *nats.Connection, local_indexes: *MultiIndex) Self {
    return .{
        .allocator = allocator,
        .scheduler = scheduler,
        .nats_connection = nats_connection,
        .jetstream = nats_connection.jetstream(.{}),
        .local_indexes = local_indexes,
        .consumers = std.StringHashMap(*IndexConsumer).init(allocator),
    };
}

pub fn deinit(self: *Self) void {
    // Stop cleanup process
    self.cleanup_running = false;
    if (self.cleanup_task) |task| {
        self.scheduler.destroyTask(task);
    }
    
    // Stop status subscription
    if (self.status_subscription) |sub| {
        sub.deinit();
    }
    
    // Stop all consumers
    self.consumers_mutex.lock();
    defer self.consumers_mutex.unlock();
    
    var iterator = self.consumers.iterator();
    while (iterator.next()) |entry| {
        entry.value_ptr.*.deinit();
        self.allocator.destroy(entry.value_ptr.*);
    }
    self.consumers.deinit();
    
    self.jetstream.deinit();
}

pub fn open(self: *Self) !void {
    // Initialize the status stream
    try self.ensureStatusStream();
    
    // Subscribe to status stream for index lifecycle management
    try self.subscribeToStatusStream();
    
    // Discover existing indexes and start their consumers
    try self.discoverExistingIndexes();
    
    // Start cleanup process
    try self.startCleanupProcess();
}

fn ensureStatusStream(self: *Self) !void {
    const stream_config = nats.StreamConfig{
        .name = STATUS_STREAM_NAME,
        .subjects = &.{"fpindex.status.*"},
        .retention = .limits,
        .storage = .file,
        .max_msgs_per_subject = 3, // Keep last 3 status updates per index
        .duplicate_window = 30 * std.time.ns_per_s, // 30 seconds
    };
    
    // Try to create or update the stream
    self.jetstream.addStream(stream_config) catch |err| switch (err) {
        error.StreamAlreadyExists => {
            // Update existing stream with new configuration
            _ = try self.jetstream.updateStream(stream_config);
        },
        else => return err,
    };
    
    log.info("status stream {s} initialized", .{STATUS_STREAM_NAME});
}

fn subscribeToStatusStream(self: *Self) !void {
    const consumer_config = nats.ConsumerConfig{
        .ack_policy = .explicit,
        .deliver_policy = .all, // Start from beginning to catch all index statuses
        .filter_subject = "fpindex.status.*",
    };
    
    self.status_subscription = try self.jetstream.subscribe(
        STATUS_STREAM_NAME,
        consumer_config,
        statusMessageHandler,
        .{self}
    );
}

fn statusMessageHandler(js_msg: *nats.JetStreamMessage, context: *Self) void {
    defer js_msg.deinit();
    
    context.processStatusMessage(js_msg) catch |err| {
        log.err("failed to process status message: {}", .{err});
        js_msg.nak() catch {};
        return;
    };
    
    js_msg.ack() catch |err| {
        log.err("failed to ack status message: {}", .{err});
    };
}

fn processStatusMessage(self: *Self, js_msg: *nats.JetStreamMessage) !void {
    // Extract index name from subject (fpindex.status.{index_name})
    const subject_parts = std.mem.split(u8, js_msg.subject, ".");
    _ = subject_parts.next(); // skip "fpindex"
    _ = subject_parts.next(); // skip "status"
    const index_name = subject_parts.next() orelse return error.InvalidSubject;
    
    // Decode status message
    const status = try IndexStatus.decode(js_msg.data, self.allocator);
    
    log.debug("received status update for index {s}: {s}", .{ index_name, @tagName(status.state) });
    
    switch (status.state) {
        .creating => {
            // Index is being created, don't start consumer yet
        },
        .created => {
            // Start consumer for this index if not already running
            try self.startIndexConsumer(index_name);
        },
        .deleting => {
            // Stop consumer for this index
            try self.stopIndexConsumer(index_name);
        },
        .deleted => {
            // Ensure consumer is stopped and cleaned up
            try self.stopIndexConsumer(index_name);
        },
    }
}

fn discoverExistingIndexes(self: *Self) !void {
    // Get all messages from status stream to discover existing indexes
    var stream_info = try self.jetstream.getStreamInfo(STATUS_STREAM_NAME);
    defer stream_info.deinit();
    
    // For each subject in the stream, get the latest message
    // Note: This is a simplified approach - in practice you'd want to
    // iterate through all subjects more efficiently
    log.info("discovered {} messages in status stream", .{stream_info.state.messages});
}

fn startIndexConsumer(self: *Self, index_name: []const u8) !void {
    self.consumers_mutex.lock();
    defer self.consumers_mutex.unlock();
    
    // Check if consumer already exists
    if (self.consumers.get(index_name)) |consumer| {
        if (consumer.is_running) {
            return; // Already running
        }
        consumer.stop(); // Stop old consumer
    }
    
    // Create new consumer
    const consumer = try self.allocator.create(IndexConsumer);
    errdefer self.allocator.destroy(consumer);
    
    consumer.* = IndexConsumer{
        .allocator = self.allocator,
        .index_name = try self.allocator.dupe(u8, index_name),
        .consumer_name = try getConsumerName(self.allocator, index_name),
    };
    
    // Set up the consumer
    try self.setupIndexConsumer(consumer);
    
    // Add to consumers map
    try self.consumers.put(index_name, consumer);
    
    log.info("started consumer for index {s}", .{index_name});
}

fn setupIndexConsumer(self: *Self, consumer: *IndexConsumer) !void {
    const stream_name = try getUpdatesStreamName(self.allocator, consumer.index_name);
    defer self.allocator.free(stream_name);
    
    const subject = try getUpdatesSubject(self.allocator, consumer.index_name);
    defer self.allocator.free(subject);
    
    // Check if index exists locally and get its last sequence number
    const local_last_seq = self.getLocalIndexLastSeq(consumer.index_name) catch 0;
    
    // Check if consumer exists and reconcile
    const consumer_info = self.jetstream.getConsumerInfo(stream_name, consumer.consumer_name) catch |err| switch (err) {
        error.ConsumerNotFound => {
            // Create new consumer starting from where local index left off
            const consumer_config = nats.ConsumerConfig{
                .durable_name = consumer.consumer_name,
                .ack_policy = .explicit,
                .deliver_policy = if (local_last_seq > 0) .by_start_sequence else .all,
                .opt_start_seq = if (local_last_seq > 0) local_last_seq + 1 else null,
                .filter_subject = subject,
                .max_deliver = 3,
                .ack_wait = 30 * std.time.ns_per_s,
            };
            
            _ = try self.jetstream.addConsumer(stream_name, consumer_config);
            null
        },
        else => return err,
    } orelse try self.jetstream.getConsumerInfo(stream_name, consumer.consumer_name);
    
    if (consumer_info) |info| {
        defer info.deinit();
        
        // Reconcile consumer state with local index
        if (info.delivered.stream_seq != local_last_seq) {
            log.warn("consumer {s} sequence mismatch: consumer={}, local={}", .{
                consumer.consumer_name, info.delivered.stream_seq, local_last_seq
            });
            
            if (local_last_seq > info.delivered.stream_seq) {
                // Local index is ahead - this shouldn't happen normally
                // For safety, delete local index and consumer, start fresh
                log.warn("local index ahead of consumer, resetting both");
                try self.local_indexes.deleteIndex(consumer.index_name);
                try self.jetstream.deleteConsumer(stream_name, consumer.consumer_name);
                
                // Recreate consumer from beginning
                const consumer_config = nats.ConsumerConfig{
                    .durable_name = consumer.consumer_name,
                    .ack_policy = .explicit,
                    .deliver_policy = .all,
                    .filter_subject = subject,
                    .max_deliver = 3,
                    .ack_wait = 30 * std.time.ns_per_s,
                };
                
                _ = try self.jetstream.addConsumer(stream_name, consumer_config);
            }
        }
        
        consumer.last_processed_seq = info.delivered.stream_seq;
    }
    
    // Create pull subscription
    const consumer_config = nats.ConsumerConfig{
        .durable_name = consumer.consumer_name,
    };
    
    consumer.subscription = try self.jetstream.pullSubscribe(stream_name, consumer_config);
    
    // Start consumer thread
    consumer.is_running = true;
    consumer.thread = try std.Thread.spawn(.{}, runIndexConsumer, .{ self, consumer });
}

fn runIndexConsumer(self: *Self, consumer: *IndexConsumer) void {
    while (consumer.is_running) {
        // Fetch messages in small batches
        const batch = consumer.subscription.?.fetch(10, 5000) catch |err| {
            log.err("failed to fetch messages for index {s}: {}", .{ consumer.index_name, err });
            std.time.sleep(1 * std.time.ns_per_s); // Wait before retrying
            continue;
        };
        defer batch.deinit();
        
        for (batch.messages) |msg| {
            defer msg.deinit();
            
            self.processUpdateMessage(consumer.index_name, msg) catch |err| {
                log.err("failed to process update for index {s}: {}", .{ consumer.index_name, err });
                msg.nak() catch {};
                continue;
            };
            
            msg.ack() catch |err| {
                log.err("failed to ack message for index {s}: {}", .{ consumer.index_name, err });
            };
            
            consumer.last_processed_seq = msg.seq;
        }
        
        if (batch.messages.len == 0) {
            // No messages, sleep briefly
            std.time.sleep(100 * std.time.ns_per_ms);
        }
    }
}

fn processUpdateMessage(self: *Self, index_name: []const u8, msg: *nats.JetStreamMessage) !void {
    const update = try UpdateMessage.decode(msg.data, self.allocator);
    defer {
        switch (update) {
            .create_index => |create| self.allocator.free(create.index_name),
            .delete_index => |delete| self.allocator.free(delete.index_name),
            .fingerprint_update => |fp_update| {
                // Note: Changes and metadata contain pointers that need cleanup
                // This is simplified - in practice you'd need more careful memory management
                self.allocator.free(fp_update.changes);
                self.allocator.free(fp_update.metadata);
            },
        }
    }
    
    switch (update) {
        .create_index => |create| {
            if (!std.mem.eql(u8, create.index_name, index_name)) {
                return error.IndexNameMismatch;
            }
            // Index creation is handled by status stream
            log.debug("received create_index message for {s}", .{index_name});
        },
        .delete_index => |delete| {
            if (!std.mem.eql(u8, delete.index_name, index_name)) {
                return error.IndexNameMismatch;
            }
            // Delete local index and stop consumer
            try self.local_indexes.deleteIndex(index_name);
            try self.stopIndexConsumer(index_name);
            log.info("deleted local index {s} and stopped consumer", .{index_name});
        },
        .fingerprint_update => |fp_update| {
            // Apply update to local index
            _ = try self.local_indexes.update(self.allocator, index_name, .{
                .changes = fp_update.changes,
                .metadata = fp_update.metadata,
                .expected_version = fp_update.expected_version,
            });
        },
    }
}

fn stopIndexConsumer(self: *Self, index_name: []const u8) !void {
    self.consumers_mutex.lock();
    defer self.consumers_mutex.unlock();
    
    if (self.consumers.fetchRemove(index_name)) |entry| {
        defer self.allocator.destroy(entry.value);
        entry.value.deinit();
        log.info("stopped consumer for index {s}", .{index_name});
    }
}

fn getLocalIndexLastSeq(self: *Self, index_name: []const u8) !u64 {
    const index = self.local_indexes.getIndex(index_name) catch return 0;
    defer self.local_indexes.releaseIndex(index);
    
    var reader = try index.acquireReader();
    defer index.releaseReader(&reader);
    
    // Return version as sequence number
    return reader.getVersion();
}

// Public API methods that delegate to local MultiIndex
pub fn createIndex(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
) !api.CreateIndexResponse {
    // First publish status change to creating
    try self.publishIndexStatus(index_name, .creating);
    
    // Create the updates stream for this index
    try self.ensureIndexUpdatesStream(index_name);
    
    // Create local index
    const result = try self.local_indexes.createIndex(allocator, index_name);
    
    // Publish create_index message to updates stream
    const update_msg = UpdateMessage{ .create_index = .{ .index_name = index_name } };
    try self.publishUpdateMessage(index_name, update_msg);
    
    // Publish status change to created
    try self.publishIndexStatus(index_name, .created);
    
    return result;
}

pub fn deleteIndex(self: *Self, name: []const u8) !void {
    // Publish status change to deleting
    try self.publishIndexStatus(name, .deleting);
    
    // Publish delete_index message
    const update_msg = UpdateMessage{ .delete_index = .{ .index_name = name } };
    try self.publishUpdateMessage(name, update_msg);
    
    // Wait for consumers to stop (simplified - should check consumer count)
    std.time.sleep(1 * std.time.ns_per_s);
    
    // Delete local index
    try self.local_indexes.deleteIndex(name);
    
    // Clean up updates stream
    const stream_name = try getUpdatesStreamName(self.allocator, name);
    defer self.allocator.free(stream_name);
    
    self.jetstream.deleteStream(stream_name) catch |err| {
        log.warn("failed to delete updates stream {s}: {}", .{ stream_name, err });
    };
    
    // Publish final status change to deleted
    try self.publishIndexStatus(name, .deleted);
}

pub fn search(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
    request: api.SearchRequest,
) !api.SearchResponse {
    // Check index status before allowing search
    const status = try self.getIndexStatus(index_name);
    if (status.state != .created) {
        return error.IndexNotAvailable;
    }
    
    return self.local_indexes.search(allocator, index_name, request);
}

pub fn update(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
    request: api.UpdateRequest,
) !api.UpdateResponse {
    // Check index status before allowing updates
    const status = try self.getIndexStatus(index_name);
    if (status.state != .created) {
        return error.IndexNotAvailable;
    }
    
    // Publish update message to cluster
    const update_msg = UpdateMessage{ .fingerprint_update = .{
        .changes = request.changes,
        .metadata = request.metadata,
        .expected_version = request.expected_version,
    } };
    try self.publishUpdateMessage(index_name, update_msg);
    
    // Update will be applied via consumer
    // For now, return optimistic response
    // TODO: Consider implementing synchronous updates for immediate consistency
    const result = try self.local_indexes.update(allocator, index_name, request);
    return result;
}

// Remaining API methods delegate directly to local indexes
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

// Helper methods for NATS operations
fn ensureIndexUpdatesStream(self: *Self, index_name: []const u8) !void {
    const stream_name = try getUpdatesStreamName(self.allocator, index_name);
    defer self.allocator.free(stream_name);
    
    const subject = try getUpdatesSubject(self.allocator, index_name);
    defer self.allocator.free(subject);
    
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{subject},
        .retention = .limits,
        .storage = .file,
        .max_msgs = 10_000_000, // High limit for updates
        .max_bytes = 10 * 1024 * 1024 * 1024, // 10GB
        .duplicate_window = 60 * std.time.ns_per_s, // 1 minute
    };
    
    self.jetstream.addStream(stream_config) catch |err| switch (err) {
        error.StreamAlreadyExists => {
            // Stream already exists, which is fine
        },
        else => return err,
    };
    
    log.debug("ensured updates stream {s} exists", .{stream_name});
}

fn publishIndexStatus(self: *Self, index_name: []const u8, state: IndexState) !void {
    const subject = try getStatusSubject(self.allocator, index_name);
    defer self.allocator.free(subject);
    
    const status = IndexStatus{
        .state = state,
        .version = 0, // TODO: Get actual version
        .timestamp = std.time.timestamp(),
    };
    
    const message_data = try status.encode(self.allocator);
    defer self.allocator.free(message_data);
    
    _ = try self.jetstream.publish(subject, message_data);
    log.debug("published status {s} for index {s}", .{ @tagName(state), index_name });
}

fn publishUpdateMessage(self: *Self, index_name: []const u8, update_msg: UpdateMessage) !void {
    const subject = try getUpdatesSubject(self.allocator, index_name);
    defer self.allocator.free(subject);
    
    const message_data = try update_msg.encode(self.allocator);
    defer self.allocator.free(message_data);
    
    _ = try self.jetstream.publish(subject, message_data);
    log.debug("published update message for index {s}", .{index_name});
}

fn getIndexStatus(self: *Self, index_name: []const u8) !IndexStatus {
    const subject = try getStatusSubject(self.allocator, index_name);
    defer self.allocator.free(subject);
    
    // Get latest status message for this index
    const msg = try self.jetstream.getLastMsg(STATUS_STREAM_NAME, subject);
    defer msg.deinit();
    
    return try IndexStatus.decode(msg.data, self.allocator);
}

// Cleanup process implementation
fn startCleanupProcess(self: *Self) !void {
    self.cleanup_running = true;
    self.cleanup_task = try self.scheduler.createTask(.low, cleanupTask, .{self});
    if (self.cleanup_task) |task| {
        self.scheduler.scheduleTask(task);
    }
    log.info("started cleanup process");
}

fn cleanupTask(self: *Self) void {
    self.cleanupDeletedIndexes() catch |err| {
        log.err("cleanup task failed: {}", .{err});
    };
    
    // Reschedule for next cleanup (every 5 minutes)
    if (self.cleanup_running and self.cleanup_task != null) {
        std.time.sleep(5 * 60 * std.time.ns_per_s); // 5 minutes
        if (self.cleanup_running) {
            self.scheduler.scheduleTask(self.cleanup_task.?);
        }
    }
}

fn cleanupDeletedIndexes(self: *Self) !void {
    log.debug("running cleanup of deleted indexes");
    
    // Get all streams that match our updates pattern
    var stream_list = try self.jetstream.listStreamNames();
    defer stream_list.deinit();
    
    for (stream_list.names) |stream_name| {
        defer self.allocator.free(stream_name);
        
        // Check if it's an updates stream
        if (!std.mem.startsWith(u8, stream_name, "fpindex-updates-")) {
            continue;
        }
        
        // Extract index name from stream name
        const index_name = stream_name["fpindex-updates-".len..];
        
        // Get index status
        const status = self.getIndexStatus(index_name) catch |err| switch (err) {
            error.MessageNotFound => {
                // No status message, this shouldn't happen
                log.warn("updates stream {s} exists but no status found", .{stream_name});
                continue;
            },
            else => {
                log.err("failed to get status for index {s}: {}", .{ index_name, err });
                continue;
            },
        };
        
        if (status.state != .deleting) {
            continue;
        }
        
        // Index is in deleting state, check if it has any consumers
        const has_consumers = self.streamHasConsumers(stream_name) catch |err| {
            log.err("failed to check consumers for stream {s}: {}", .{ stream_name, err });
            continue;
        };
        
        if (!has_consumers) {
            // No consumers left, safe to delete the stream
            log.info("deleting updates stream {s} (no consumers remaining)", .{stream_name});
            
            self.jetstream.deleteStream(stream_name) catch |err| {
                log.err("failed to delete stream {s}: {}", .{ stream_name, err });
                continue;
            };
            
            // Update index status to deleted
            self.publishIndexStatus(index_name, .deleted) catch |err| {
                log.err("failed to publish deleted status for index {s}: {}", .{ index_name, err });
            };
        } else {
            log.debug("stream {s} still has consumers, skipping cleanup", .{stream_name});
        }
    }
}

fn streamHasConsumers(self: *Self, stream_name: []const u8) !bool {
    var consumer_list = self.jetstream.listConsumerNames(stream_name) catch |err| switch (err) {
        error.StreamNotFound => return false,
        else => return err,
    };
    defer consumer_list.deinit();
    
    return consumer_list.names.len > 0;
}

// Advisory message handling for consumer deletion notifications
// Note: This is a simplified implementation since the NATS client doesn't 
// have built-in advisory support. In a full implementation, you might
// subscribe to JetStream advisory subjects like "$JS.EVENT.CONSUMER.DELETE"
fn handleConsumerDeletionAdvisory(self: *Self, stream_name: []const u8, consumer_name: []const u8) !void {
    _ = consumer_name; // unused for now
    
    // When we detect a consumer deletion, check if the stream should be cleaned up
    if (std.mem.startsWith(u8, stream_name, "fpindex-updates-")) {
        const index_name = stream_name["fpindex-updates-".len..];
        
        const status = self.getIndexStatus(index_name) catch return;
        if (status.state == .deleting) {
            // Trigger immediate cleanup check for this specific stream
            const has_consumers = try self.streamHasConsumers(stream_name);
            if (!has_consumers) {
                log.info("last consumer deleted for {s}, cleaning up stream", .{stream_name});
                try self.jetstream.deleteStream(stream_name);
                try self.publishIndexStatus(index_name, .deleted);
            }
        }
    }
}