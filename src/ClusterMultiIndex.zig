const std = @import("std");
const log = std.log.scoped(.cluster_multi_index);
const nats = @import("nats");
const msgpack = @import("msgpack");

const MultiIndex = @import("MultiIndex.zig");
const api = @import("api.zig");
const Change = @import("change.zig").Change;
const Metadata = @import("Metadata.zig");
const index_redirect = @import("index_redirect.zig");
const IndexRedirect = index_redirect.IndexRedirect;

const Self = @This();

fn isValidIndexName(name: []const u8) bool {
    if (name.len == 0) return false;
    for (name, 0..) |c, i| {
        if (i == 0) {
            switch (c) {
                '0'...'9', 'A'...'Z', 'a'...'z' => {},
                else => return false,
            }
        } else {
            switch (c) {
                '0'...'9', 'A'...'Z', 'a'...'z', '-', '_' => {},
                else => return false,
            }
        }
    }
    return true;
}

const META_INDEX_NAME = "_meta";

// Stream and subject constants
const META_STREAM_NAME = "fpindex-meta";
const UPDATES_STREAM_NAME = "fpindex-updates";
const META_SUBJECT_PREFIX = "fpindex.m.";
const UPDATES_SUBJECT_PREFIX = "fpindex.u.";

const IndexStatus = struct {
    is_active: bool,
    generation: u64,
};

// Core state
allocator: std.mem.Allocator,
nc: *nats.Connection,
local_indexes: *MultiIndex,
replica_id: []const u8,
stopping: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

// State tracking
last_applied_meta_seq: u64 = 0, // Last applied meta operation sequence
mutex: std.Thread.Mutex = .{},

// NATS JetStream
js: nats.JetStream = undefined,
meta_subscription: ?*nats.JetStreamSubscription = null,
index_updaters: std.StringHashMap(*IndexUpdater),

/// Index updater for handling per-index update messages
pub const IndexUpdater = struct {
    subscription: *nats.JetStreamSubscription,
    last_applied_seq: u64,
    mutex: std.Thread.Mutex = .{},

    pub fn destroy(self: *IndexUpdater, allocator: std.mem.Allocator) void {
        // Clean up subscription first - no synchronization needed
        self.subscription.deinit();
        
        // Brief critical section to ensure no concurrent access
        self.mutex.lock();
        self.mutex.unlock();
        
        // Now safe to free the struct
        allocator.destroy(self);
    }
};

/// Metadata operation (create/delete)
pub const MetaOp = union(enum) {
    create: struct {
        index_name: []const u8,
        previous_generation: u64 = 0,
        first_seq: u64,
    },
    delete: struct {
        index_name: []const u8,
        previous_generation: u64,
    },

    pub fn msgpackFormat() msgpack.UnionFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

/// Update operation
pub const UpdateOp = struct {
    changes: []const Change,
    metadata: ?Metadata = null,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub fn init(allocator: std.mem.Allocator, nc: *nats.Connection, local_indexes: *MultiIndex) !Self {
    const replica_id = try loadOrCreateReplicaId(allocator, local_indexes);
    return .{
        .allocator = allocator,
        .nc = nc,
        .local_indexes = local_indexes,
        .replica_id = replica_id,
        .index_updaters = std.StringHashMap(*IndexUpdater).init(allocator),
    };
}

fn loadOrCreateReplicaId(allocator: std.mem.Allocator, local_indexes: *MultiIndex) ![]const u8 {
    // Get or create the _meta index (local only, not replicated)
    const index = local_indexes.getOrCreateIndex(META_INDEX_NAME, true, .{}) catch |err| {
        log.warn("failed to get/create _meta index: {}", .{err});
        return err;
    };
    defer local_indexes.releaseIndex(index);

    // Try to read existing replica_id from metadata
    var reader = try index.acquireReader();
    defer index.releaseReader(&reader);

    var metadata = try reader.getMetadata(allocator);
    defer metadata.deinit();

    if (metadata.get("cluster.replica_id")) |existing_id| {
        // Found existing replica_id, use it
        log.info("loaded existing replica_id: {s}", .{existing_id});
        return try allocator.dupe(u8, existing_id);
    }

    // No existing replica_id, generate a new one
    const new_id = try nats.nuid.nextString(allocator);
    errdefer allocator.free(new_id);

    // Store the new replica_id in metadata
    var new_metadata = Metadata.initBorrowed(allocator);
    defer new_metadata.deinit();
    try new_metadata.set("cluster.replica_id", new_id);

    _ = index.update(&[_]Change{}, new_metadata, .{}) catch |err| {
        log.warn("failed to store replica_id in _meta index: {}", .{err});
        return err;
    };

    log.info("generated new replica_id: {s}", .{new_id});
    return new_id;
}

pub fn deinit(self: *Self) void {
    self.stop();

    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.meta_subscription) |sub| {
        sub.deinit();
    }
    self.meta_subscription = undefined;

    var iter = self.index_updaters.iterator();
    while (iter.next()) |entry| {
        entry.value_ptr.*.destroy(self.allocator);
        self.allocator.free(entry.key_ptr.*);
    }
    self.index_updaters.deinit();
    self.index_updaters = undefined;

    self.allocator.free(self.replica_id);
    self.replica_id = undefined;
}

pub fn start(self: *Self) !void {
    // Initialize JetStream
    self.js = nats.JetStream.init(self.nc, .{});

    // Ensure stream exists
    try self.ensureStream();

    // Load existing indexes and their last applied sequences
    try self.loadExistingIndexes();

    // Create meta consumer for index create/delete operations
    try self.createMetaConsumer();

    // Start updaters for existing indexes
    try self.startExistingIndexUpdaters();
}

pub fn stop(self: *Self) void {
    // Mark as stopping
    self.stopping.store(true, .release);

    // Drain subscriptions
    self.drainMetaSubscription();
    self.drainUpdateSubscriptions();

    // Try to wait for them to finish, best effort
    self.waitForMetaSubscriptionDrained(30 * std.time.ms_per_s);
    self.waitForUpdateSubscriptionsDrained(30 * std.time.ms_per_s);
}

fn drainMetaSubscription(self: *Self) void {
    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.meta_subscription) |sub| {
        sub.subscription.drain();
    }
}

fn drainUpdateSubscriptions(self: *Self) void {
    self.mutex.lock();
    defer self.mutex.unlock();

    var iter = self.index_updaters.iterator();
    while (iter.next()) |entry| {
        const updater = entry.value_ptr.*;
        updater.subscription.subscription.drain();
    }
}

fn waitForMetaSubscriptionDrained(self: *Self, timeout_ms: u64) void {
    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.meta_subscription) |js_sub| {
        const sub = js_sub.subscription;

        self.mutex.unlock();
        defer self.mutex.lock();

        sub.waitForDrainCompletion(timeout_ms) catch |err| {
            log.warn("failed to wait for meta subscription drain completion: {}", .{err});
            return;
        };
    }
}

fn waitForUpdateSubscriptionsDrained(self: *Self, timeout_ms: u64) void {
    self.mutex.lock();
    defer self.mutex.unlock();

    var iter = self.index_updaters.iterator();
    while (iter.next()) |entry| {
        const sub = entry.value_ptr.*.subscription.subscription;

        self.mutex.unlock();
        defer self.mutex.lock();

        sub.waitForDrainCompletion(timeout_ms) catch |err| {
            log.warn("failed to wait for update subscription drain completion: {}", .{err});
            continue;
        };
    }
}

fn getStatus(self: *Self, index_name: []const u8) !IndexStatus {
    const subject = try std.fmt.allocPrint(self.allocator, META_SUBJECT_PREFIX ++ "{s}", .{index_name});
    defer self.allocator.free(subject);

    const msg = self.js.getMsg(META_STREAM_NAME, .{ .last_by_subj = subject, .direct = true }) catch |err| switch (err) {
        error.MessageNotFound => return IndexStatus{ .is_active = false, .generation = 0 },
        else => return err,
    };
    defer msg.deinit();

    var result = try msgpack.decodeFromSlice(MetaOp, self.allocator, msg.data);
    defer result.deinit();

    const meta_op = result.value;

    switch (meta_op) {
        .create => return IndexStatus{ .is_active = true, .generation = msg.seq },
        .delete => return IndexStatus{ .is_active = false, .generation = msg.seq },
    }
}

fn getLastVersion(self: *Self, index_name: []const u8, generation: u64) !u64 {
    // Get the last sequence number for this index's updates
    const update_subject = try std.fmt.allocPrint(self.allocator, UPDATES_SUBJECT_PREFIX ++ "{s}.{d}", .{ index_name, generation });
    defer self.allocator.free(update_subject);

    const last_update_msg = self.js.getMsg(UPDATES_STREAM_NAME, .{ .last_by_subj = update_subject, .direct = true }) catch |err| switch (err) {
        error.MessageNotFound => {
            // No updates yet, return the generation (creation sequence)
            return generation;
        },
        else => return err,
    };
    defer last_update_msg.deinit();

    return last_update_msg.seq;
}

fn ensureStream(self: *Self) !void {
    // Create meta stream
    const meta_stream_config = nats.StreamConfig{
        .name = META_STREAM_NAME,
        .subjects = &[_][]const u8{META_SUBJECT_PREFIX ++ ">"},
        .retention = .limits,
        .storage = .file,
        .num_replicas = 1,
        .allow_direct = true,
        .allow_rollup_hdrs = true,
        .discard = .new,
        .duplicate_window = 10 * std.time.ns_per_s,
        .max_msgs_per_subject = 1,
    };

    var meta_stream_info = self.js.addStream(meta_stream_config) catch |err| switch (err) {
        error.StreamNameExist => null, // Already exists, ignore
        else => return err,
    };
    defer if (meta_stream_info) |*info| info.deinit();

    // Create updates stream
    const updates_stream_config = nats.StreamConfig{
        .name = UPDATES_STREAM_NAME,
        .subjects = &[_][]const u8{UPDATES_SUBJECT_PREFIX ++ ">"},
        .retention = .limits,
        .storage = .file,
        .num_replicas = 1,
        .allow_direct = true,
        .allow_rollup_hdrs = true,
        .discard = .new,
        .duplicate_window = 10 * std.time.ns_per_s,
    };

    var updates_stream_info = self.js.addStream(updates_stream_config) catch |err| switch (err) {
        error.StreamNameExist => null, // Already exists, ignore
        else => return err,
    };
    defer if (updates_stream_info) |*info| info.deinit();
}

fn loadExistingIndexes(self: *Self) !void {
    self.mutex.lock();
    defer self.mutex.unlock();

    // Get list of existing indexes from MultiIndex (including deleted ones)
    const index_list = try self.local_indexes.listIndexes(self.allocator, .{ .include_deleted = true });
    defer self.allocator.free(index_list);

    for (index_list) |info| {
        // Skip system indexes
        if (std.mem.eql(u8, info.name, META_INDEX_NAME)) {
            continue;
        }

        var last_seq: u64 = undefined;
        if (info.deleted) {
            // Use generation as last_seq as that's when it has been deleted
            last_seq = info.generation;
            log.info("loaded deleted index {s} (generation={}, last_seq={})", .{ info.name, info.generation, last_seq });
        } else {
            // For active indexes, try to get the actual last applied sequence
            const index = self.local_indexes.getIndex(info.name) catch |err| {
                log.warn("failed to get index {s} during startup: {}", .{ info.name, err });
                return err;
            };
            defer self.local_indexes.releaseIndex(index);

            var reader = index.acquireReader() catch |err| {
                log.warn("failed to acquire reader for index {s} during startup: {}", .{ info.name, err });
                return err;
            };
            defer index.releaseReader(&reader);

            last_seq = reader.getVersion();
            if (last_seq == 0) {
                // It can be 0 only if the initial empty commit during index creation failed,
                // however generation represents the same message sequence.
                last_seq = info.generation;
            }
            log.info("loaded active index {s} (generation={}, last_seq={})", .{ info.name, info.generation, last_seq });
        }

        // Update last_applied_meta_seq by checking the generation (which is the meta sequence)
        // For active indexes, the generation represents when they were created
        // For deleted indexes, the generation represents when they were deleted
        self.last_applied_meta_seq = @max(self.last_applied_meta_seq, info.generation);
    }

    log.info("last_applied_meta_seq: {}", .{self.last_applied_meta_seq});
}

fn createMetaConsumer(self: *Self) !void {
    const consumer_name = try std.fmt.allocPrint(self.allocator, "replica-{s}-meta", .{self.replica_id});
    defer self.allocator.free(consumer_name);

    const consumer_config = nats.ConsumerConfig{
        .durable_name = consumer_name,
        .ack_policy = .explicit,
        .deliver_policy = .all,
        .filter_subject = META_SUBJECT_PREFIX ++ "*",
        .max_ack_pending = 1,
        .ack_wait = 60 * std.time.ns_per_s,
    };

    self.meta_subscription = try self.js.subscribe(META_STREAM_NAME, consumer_config, handleMetaMessage, .{self});
}

fn startExistingIndexUpdaters(self: *Self) !void {
    self.mutex.lock();
    defer self.mutex.unlock();

    const index_list = try self.local_indexes.listIndexes(self.allocator, .{});
    defer self.allocator.free(index_list);

    for (index_list) |info| {
        // Skip system indexes and deleted indexes
        if (std.mem.eql(u8, info.name, META_INDEX_NAME) or info.deleted) {
            continue;
        }

        // Get the current version for the index
        const index = self.local_indexes.getIndex(info.name) catch |err| {
            log.warn("failed to get index {s} during startup: {}", .{ info.name, err });
            continue;
        };
        defer self.local_indexes.releaseIndex(index);

        var reader = index.acquireReader() catch |err| {
            log.warn("failed to acquire reader for index {s} during startup: {}", .{ info.name, err });
            continue;
        };
        defer index.releaseReader(&reader);

        const last_seq = reader.getVersion();
        const final_seq = if (last_seq == 0) info.generation else last_seq;

        try self.startIndexUpdater(info.name, info.generation, final_seq);
    }
}

fn startIndexUpdater(self: *Self, index_name: []const u8, generation: u64, last_seq: u64) !void {
    // Caller must hold self.mutex

    const consumer_name = try std.fmt.allocPrint(self.allocator, "replica-{s}-{s}", .{ self.replica_id, index_name });
    defer self.allocator.free(consumer_name);

    const filter_subject = try std.fmt.allocPrint(self.allocator, UPDATES_SUBJECT_PREFIX ++ "{s}.{d}", .{ index_name, generation });
    defer self.allocator.free(filter_subject);

    const consumer_config = nats.ConsumerConfig{
        .durable_name = consumer_name,
        .ack_policy = .explicit,
        .deliver_policy = .by_start_sequence,
        .opt_start_seq = last_seq + 1,
        .filter_subject = filter_subject,
        .max_ack_pending = 1,
        .ack_wait = 60 * std.time.ns_per_s,
    };

    const subscription = try self.js.subscribe(UPDATES_STREAM_NAME, consumer_config, handleUpdateMessage, .{self});

    const result = try self.index_updaters.getOrPut(index_name);
    if (result.found_existing) {
        std.debug.panic("startIndexUpdater called for existing index: {s}", .{index_name});
    } else {
        // New entry - need to allocate key
        result.key_ptr.* = try self.allocator.dupe(u8, index_name);
    }

    // Allocate and set the new value
    const updater = try self.allocator.create(IndexUpdater);
    updater.* = IndexUpdater{
        .subscription = subscription,
        .last_applied_seq = last_seq,
        .mutex = .{},
    };
    result.value_ptr.* = updater;

    log.info("started updater for index {s} (generation={}, start_seq={})", .{ index_name, generation, last_seq + 1 });
}

fn getIndexUpdater(self: *Self, index_name: []const u8) ?*IndexUpdater {
    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.index_updaters.get(index_name)) |updater| {
        updater.mutex.lock();
        return updater;
    }
    return null;
}

fn releaseIndexUpdater(self: *Self, updater: *IndexUpdater) void {
    _ = self; // unused
    updater.mutex.unlock();
}

fn stopIndexUpdater(self: *Self, index_name: []const u8) !void {
    // Caller must hold self.mutex

    if (self.index_updaters.fetchRemove(index_name)) |entry| {
        entry.value.destroy(self.allocator);
        self.allocator.free(entry.key);
    }
}

fn handleMetaMessage(js_msg: *nats.JetStreamMessage, self: *Self) void {
    defer js_msg.deinit();

    if (self.stopping.load(.acquire)) {
        log.debug("ignoring meta message '{s}' while stopping", .{js_msg.msg.subject});
        return;
    }

    // Parse subject to get index name
    if (!std.mem.startsWith(u8, js_msg.msg.subject, META_SUBJECT_PREFIX)) return;
    const parts_str = js_msg.msg.subject[META_SUBJECT_PREFIX.len..];
    var parts = std.mem.splitSequence(u8, parts_str, ".");
    const index_name = parts.next() orelse return;
    if (parts.next() != null) {
        log.warn("unexpected subject format: {s}", .{js_msg.msg.subject});
        return;
    }

    self.processMetaOperation(index_name, js_msg) catch |err| {
        log.err("failed to process meta operation for {s}: {}", .{ index_name, err });
        // Don't ACK on error - message will be redelivered
        return;
    };

    // ACK only on successful processing
    js_msg.ack() catch |err| {
        log.err("failed to ack meta message: {}", .{err});
    };
}

fn handleUpdateMessage(js_msg: *nats.JetStreamMessage, self: *Self) void {
    defer js_msg.deinit();

    if (self.stopping.load(.acquire)) {
        log.debug("ignoring update message '{s}' while stopping", .{js_msg.msg.subject});
        return;
    }

    // Parse subject: fpindex.u.{index}.{generation}
    if (!std.mem.startsWith(u8, js_msg.msg.subject, UPDATES_SUBJECT_PREFIX)) return;
    const parts_str = js_msg.msg.subject[UPDATES_SUBJECT_PREFIX.len..];
    var parts = std.mem.splitSequence(u8, parts_str, ".");
    const index_name = parts.next() orelse return;
    const generation_str = parts.next() orelse return;

    const generation = std.fmt.parseInt(u64, generation_str, 10) catch {
        log.warn("invalid generation in subject {s}", .{js_msg.msg.subject});
        return;
    };
    if (parts.next() != null) {
        log.warn("unexpected subject format: {s}", .{js_msg.msg.subject});
        return;
    }

    self.processUpdateOperation(index_name, generation, js_msg) catch |err| {
        log.err("failed to process update for {s}: {}", .{ index_name, err });
        // Don't ACK on error - message will be redelivered
        return;
    };

    // ACK only on successful processing
    js_msg.ack() catch |err| {
        log.err("failed to ack update message: {}", .{err});
    };
}

fn processMetaOperation(self: *Self, index_name: []const u8, msg: *nats.JetStreamMessage) !void {
    const meta_op_parsed = try msgpack.decodeFromSlice(MetaOp, self.allocator, msg.msg.data);
    defer meta_op_parsed.deinit();
    const meta_op = meta_op_parsed.value;

    const generation = msg.metadata.sequence.stream;

    self.mutex.lock();
    defer self.mutex.unlock();

    // Skip if already processed
    if (generation <= self.last_applied_meta_seq) {
        log.debug("skipping already processed meta operation for index {s} (seq={}, last_applied_meta={})", .{ index_name, generation, self.last_applied_meta_seq });
        return;
    }

    switch (meta_op) {
        .create => |create_op| { // create
            // Create the index locally with the NATS generation as the version
            _ = self.local_indexes.createIndexInternal(index_name, .{
                .generation = generation,
                .expect_does_not_exist = true,
            }) catch |err| {
                log.warn("failed to create local index {s}: {}", .{ index_name, err });
                return err;
            };

            // Make an empty commit to set the initial version to first_seq from updates stream
            _ = self.local_indexes.updateInternal(self.allocator, index_name, .{
                .changes = &[_]Change{},
                .metadata = null,
                .expected_version = null,
            }, .{
                .version = create_op.first_seq,
            }) catch |err| {
                log.warn("failed to set initial version for {s}: {}", .{ index_name, err });
                // this is not critical, we can ignore it
            };

            log.info("created index {s} with generation {}", .{ index_name, generation });

            // Start updater for the new index
            self.startIndexUpdater(index_name, generation, create_op.first_seq) catch |err| {
                log.err("failed to start updater for new index {s}: {}", .{ index_name, err });
            };

            // Update meta seq
            std.debug.assert(msg.metadata.sequence.stream > self.last_applied_meta_seq);
            self.last_applied_meta_seq = msg.metadata.sequence.stream;
        },
        .delete => |delete_op| { // delete
            // Stop updater for the index
            self.stopIndexUpdater(index_name) catch |err| {
                log.err("failed to stop updater for index {s}: {}", .{ index_name, err });
            };

            // Delete local index with version validation and custom version from NATS sequence
            self.local_indexes.deleteIndexInternal(index_name, .{
                .expect_generation = delete_op.previous_generation,
                .generation = generation,
            }) catch |err| {
                log.warn("failed to delete local index {s}: {}", .{ index_name, err });
                return err;
            };

            log.info("deleted index {s} with generation {}", .{ index_name, generation });

            // Update meta seq
            std.debug.assert(msg.metadata.sequence.stream > self.last_applied_meta_seq);
            self.last_applied_meta_seq = msg.metadata.sequence.stream;
        },
    }
}

fn processUpdateOperation(self: *Self, index_name: []const u8, generation: u64, msg: *nats.JetStreamMessage) !void {
    const updater = self.getIndexUpdater(index_name) orelse {
        log.warn("no updater found for index {s}", .{index_name});
        return;
    };
    defer self.releaseIndexUpdater(updater);

    // Skip if already processed
    if (msg.metadata.sequence.stream <= updater.last_applied_seq) {
        log.debug("skipping already processed update for index {s} (seq={}, last_applied={})", .{ index_name, msg.metadata.sequence.stream, updater.last_applied_seq });
        return;
    }

    // Decode the update operation
    var update_op = try msgpack.decodeFromSlice(UpdateOp, self.allocator, msg.msg.data);
    defer update_op.deinit();

    // Apply the update to local index
    const update_request = api.UpdateRequest{
        .changes = update_op.value.changes,
        .metadata = update_op.value.metadata,
        .expected_version = null, // Version control handled at NATS level
    };

    _ = self.local_indexes.updateInternal(self.allocator, index_name, update_request, .{
        .expect_generation = generation,
        .version = msg.metadata.sequence.stream,
    }) catch |err| {
        log.err("failed to apply update to index {s}: {}", .{ index_name, err });
        return;
    };

    // Update per-index last_applied_seq
    std.debug.assert(msg.metadata.sequence.stream > updater.last_applied_seq);
    updater.last_applied_seq = msg.metadata.sequence.stream;

    log.debug("applied update to index {s} (seq={})", .{ index_name, msg.metadata.sequence.stream });
}

// Interface methods for server.zig compatibility

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
    _: std.mem.Allocator, // Not used in cluster mode
    index_name: []const u8,
    request: api.UpdateRequest,
) !api.UpdateResponse {
    if (!isValidIndexName(index_name)) {
        return error.InvalidIndexName;
    }

    // First check if index exists and get current generation
    const status = try self.getStatus(index_name);
    if (!status.is_active) {
        return error.IndexNotFound;
    }
    const generation = status.generation;

    // Prepare update operation
    const update_op = UpdateOp{
        .changes = request.changes,
        .metadata = request.metadata,
    };

    var data = std.ArrayList(u8).init(self.allocator);
    defer data.deinit();

    try msgpack.encode(update_op, data.writer());

    const update_subject = try std.fmt.allocPrint(self.allocator, UPDATES_SUBJECT_PREFIX ++ "{s}.{d}", .{ index_name, generation });
    defer self.allocator.free(update_subject);

    var publish_opts = nats.PublishOptions{};
    if (request.expected_version) |expected_version| {
        publish_opts.expected_last_subject_seq = expected_version;
    }

    const result = self.js.publish(update_subject, data.items, publish_opts) catch |err| switch (err) {
        nats.JetStreamError.StreamWrongLastSequence => return error.VersionMismatch,
        else => return err,
    };
    defer result.deinit();

    return api.UpdateResponse{ .version = result.value.seq };
}

pub fn getIndexInfo(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
) !api.GetIndexInfoResponse {
    return try self.local_indexes.getIndexInfo(allocator, index_name);
}

pub fn checkIndexExists(self: *Self, index_name: []const u8) !void {
    return self.local_indexes.checkIndexExists(index_name);
}

pub fn createIndex(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
) !api.CreateIndexResponse {
    _ = allocator;

    if (!isValidIndexName(index_name)) {
        return error.InvalidIndexName;
    }

    // First check if index exists and get current generation
    const status = try self.getStatus(index_name);
    if (status.is_active) {
        const version = try self.getLastVersion(index_name, status.generation);
        return api.CreateIndexResponse{ .version = version };
    }

    // Get the current last sequence from updates stream
    const updates_stream_info = self.js.getStreamInfo(UPDATES_STREAM_NAME) catch |err| switch (err) {
        error.StreamNotFound => blk: {
            // Updates stream doesn't exist yet, use seq 0
            break :blk null;
        },
        else => return err,
    };
    const first_seq = if (updates_stream_info) |info| blk: {
        defer info.deinit();
        break :blk info.value.state.last_seq;
    } else 0;

    // Check current status
    const subject = try std.fmt.allocPrint(self.allocator, META_SUBJECT_PREFIX ++ "{s}", .{index_name});
    defer self.allocator.free(subject);

    // Publish create operation
    const meta_op = MetaOp{
        .create = .{
            .index_name = index_name,
            .previous_generation = status.generation,
            .first_seq = first_seq,
        },
    };

    var data = std.ArrayList(u8).init(self.allocator);
    defer data.deinit();

    try msgpack.encode(meta_op, data.writer());

    const msg_id = try std.fmt.allocPrint(self.allocator, "create-{s}-{d}", .{ index_name, status.generation });
    defer self.allocator.free(msg_id);

    const result = try self.js.publish(subject, data.items, .{ .msg_id = msg_id });
    defer result.deinit();

    return api.CreateIndexResponse{ .version = result.value.seq };
}

pub fn deleteIndex(self: *Self, index_name: []const u8) !void {
    if (!isValidIndexName(index_name)) {
        return error.InvalidIndexName;
    }

    // Get current status and generation
    const status = try self.getStatus(index_name);
    if (!status.is_active) {
        return; // Already deleted
    }
    const generation = status.generation;

    // Publish delete operation
    const meta_op = MetaOp{
        .delete = .{
            .index_name = index_name,
            .previous_generation = generation,
        },
    };

    var data = std.ArrayList(u8).init(self.allocator);
    defer data.deinit();

    try msgpack.encode(meta_op, data.writer());

    const subject = try std.fmt.allocPrint(self.allocator, META_SUBJECT_PREFIX ++ "{s}", .{index_name});
    defer self.allocator.free(subject);

    const msg_id = try std.fmt.allocPrint(self.allocator, "delete-{s}-{d}", .{ index_name, generation });
    defer self.allocator.free(msg_id);

    const result = try self.js.publish(subject, data.items, .{ .msg_id = msg_id });
    defer result.deinit();
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

pub fn exportSnapshot(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
    writer: anytype,
) !void {
    return self.local_indexes.exportSnapshot(allocator, index_name, writer);
}
