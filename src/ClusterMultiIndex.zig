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

const IndexStatus = struct {
    is_active: bool,
    generation: u64,
};

// Core state
allocator: std.mem.Allocator,
nc: *nats.Connection,
local_indexes: *MultiIndex,
replica_id: []const u8,
prefix: []const u8,
meta_stream_name: []const u8,
updates_stream_name: []const u8,
stopping: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

// State tracking
mutex: std.Thread.Mutex = .{},

// NATS JetStream
js: nats.JetStream = undefined,
meta_subscription: ?*nats.JetStreamSubscription = null,
index_updaters: std.StringHashMap(*IndexUpdater),

/// Index updater for handling per-index update messages
pub const IndexUpdater = struct {
    subscription: *nats.JetStreamSubscription,
    last_applied_seq: u64,
    generation: u64,
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

pub fn init(allocator: std.mem.Allocator, nc: *nats.Connection, local_indexes: *MultiIndex, prefix: ?[]const u8) !Self {
    const replica_id = try loadOrCreateReplicaId(allocator, local_indexes);
    errdefer allocator.free(replica_id);

    const actual_prefix = prefix orelse "fpindex";
    const prefix_copy = try allocator.dupe(u8, actual_prefix);
    errdefer allocator.free(prefix_copy);

    const meta_stream_name = try std.fmt.allocPrint(allocator, "{s}-meta", .{actual_prefix});
    errdefer allocator.free(meta_stream_name);

    const updates_stream_name = try std.fmt.allocPrint(allocator, "{s}-updates", .{actual_prefix});
    errdefer allocator.free(updates_stream_name);

    return .{
        .allocator = allocator,
        .nc = nc,
        .local_indexes = local_indexes,
        .replica_id = replica_id,
        .prefix = prefix_copy,
        .meta_stream_name = meta_stream_name,
        .updates_stream_name = updates_stream_name,
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

    self.allocator.free(self.prefix);
    self.prefix = undefined;

    self.allocator.free(self.meta_stream_name);
    self.meta_stream_name = undefined;

    self.allocator.free(self.updates_stream_name);
    self.updates_stream_name = undefined;
}

pub fn start(self: *Self) !void {
    // Initialize JetStream
    self.js = nats.JetStream.init(self.nc, .{});

    // Ensure stream exists
    try self.ensureStream();

    // Create meta consumer for index create/delete operations
    try self.createMetaConsumer();
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
    const subject = try std.fmt.allocPrint(self.allocator, "{s}.m.{s}", .{ self.prefix, index_name });
    defer self.allocator.free(subject);

    const msg = self.js.getMsg(self.meta_stream_name, .{ .last_by_subj = subject, .direct = true }) catch |err| switch (err) {
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
    const update_subject = try std.fmt.allocPrint(self.allocator, "{s}.u.{s}.{d}", .{ self.prefix, index_name, generation });
    defer self.allocator.free(update_subject);

    const last_update_msg = self.js.getMsg(self.updates_stream_name, .{ .last_by_subj = update_subject, .direct = true }) catch |err| switch (err) {
        error.MessageNotFound => {
            // No updates yet for this subject: version is 0 (consumers start from seq 1).
            return 0;
        },
        else => return err,
    };
    defer last_update_msg.deinit();

    return last_update_msg.seq;
}

fn ensureStream(self: *Self) !void {
    // Create meta stream
    const meta_subject_pattern = try std.fmt.allocPrint(self.allocator, "{s}.m.>", .{self.prefix});
    defer self.allocator.free(meta_subject_pattern);

    const meta_stream_config = nats.StreamConfig{
        .name = self.meta_stream_name,
        .subjects = &[_][]const u8{meta_subject_pattern},
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
    const updates_subject_pattern = try std.fmt.allocPrint(self.allocator, "{s}.u.>", .{self.prefix});
    defer self.allocator.free(updates_subject_pattern);

    const updates_stream_config = nats.StreamConfig{
        .name = self.updates_stream_name,
        .subjects = &[_][]const u8{updates_subject_pattern},
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


fn createMetaConsumer(self: *Self) !void {
    const meta_subject_pattern = try std.fmt.allocPrint(self.allocator, "{s}.m.*", .{self.prefix});
    defer self.allocator.free(meta_subject_pattern);

    self.meta_subscription = try self.js.subscribe(meta_subject_pattern, handleMetaMessage, .{self}, .{
        .stream = self.meta_stream_name,
        .manual_ack = true,
        .config = .{
            .deliver_policy = .last_per_subject,
            .max_ack_pending = 1,
            .ack_wait = 60 * std.time.ns_per_s,
        },
    });
}


fn startIndexUpdater(self: *Self, index_name: []const u8, generation: u64, last_seq: u64) !void {
    // Caller must hold self.mutex

    // Check if updater already exists with correct generation (idempotent)
    if (self.index_updaters.get(index_name)) |existing_updater| {
        if (existing_updater.generation == generation) {
            log.debug("updater for index {s} generation {} already running", .{ index_name, generation });
            return;
        } else {
            log.info("stopping old updater for index {s} (generation {} -> {})", .{ index_name, existing_updater.generation, generation });
            try self.stopIndexUpdater(index_name);
        }
    }

    const consumer_name = try std.fmt.allocPrint(self.allocator, "{s}-{s}-g{d}", .{ self.replica_id, index_name, generation });
    defer self.allocator.free(consumer_name);

    const filter_subject = try std.fmt.allocPrint(self.allocator, "{s}.u.{s}.{d}", .{ self.prefix, index_name, generation });
    defer self.allocator.free(filter_subject);

    const subscription = try self.js.subscribe(filter_subject, handleUpdateMessage, .{self}, .{
        .stream = self.updates_stream_name,
        .durable = consumer_name,
        .manual_ack = true,
        .config = .{
            .deliver_policy = .by_start_sequence,
            .opt_start_seq = last_seq + 1,
            .max_ack_pending = 1,
            .ack_wait = 60 * std.time.ns_per_s,
        },
    });
    errdefer subscription.deinit();

    const index_name_copy = try self.allocator.dupe(u8, index_name);
    errdefer self.allocator.free(index_name_copy);

    // Allocate and set the new value
    const updater = try self.allocator.create(IndexUpdater);
    errdefer self.allocator.destroy(updater);
    updater.* = IndexUpdater{
        .subscription = subscription,
        .last_applied_seq = last_seq,
        .generation = generation,
        .mutex = .{},
    };

    try self.index_updaters.putNoClobber(index_name_copy, updater);

    log.info("started updater for index {s} (generation={}, start_seq={})", .{ index_name, generation, last_seq + 1 });
}

fn getIndexUpdater(self: *Self, index_name: []const u8, expected_generation: u64) ?*IndexUpdater {
    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.index_updaters.get(index_name)) |updater| {
        if (updater.generation == expected_generation) {
            updater.mutex.lock();
            return updater;
        } else {
            log.warn("updater for index {s} has wrong generation: expected {}, got {}", .{ index_name, expected_generation, updater.generation });
            return null;
        }
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

fn handleMetaMessage(js_msg: *nats.JetStreamMessage, self: *Self) !void {
    defer js_msg.deinit();

    if (self.stopping.load(.acquire)) {
        log.debug("ignoring meta message '{s}' while stopping", .{js_msg.msg.subject});
        return;
    }

    // Parse subject to get index name
    const index_name = self.parseMetaSubject(js_msg.msg.subject) orelse {
        log.warn("invalid meta subject format: {s}", .{js_msg.msg.subject});
        return;
    };

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

fn parseMetaSubject(self: *Self, subject: []const u8) ?[]const u8 {
    // Parse subject: {prefix}.m.{index}
    if (!std.mem.startsWith(u8, subject, self.prefix)) return null;
    var remainder = subject[self.prefix.len..];
    if (!std.mem.startsWith(u8, remainder, ".m.")) return null;
    const parts_str = remainder[3..]; // Skip ".m."

    var parts = std.mem.splitSequence(u8, parts_str, ".");
    const index_name = parts.next() orelse return null;
    if (parts.next() != null) return null; // Should be exactly 1 part

    return index_name;
}

fn parseUpdatesSubject(self: *Self, subject: []const u8) ?struct { index_name: []const u8, generation: u64 } {
    // Parse subject: {prefix}.u.{index}.{generation}
    if (!std.mem.startsWith(u8, subject, self.prefix)) return null;
    var remainder = subject[self.prefix.len..];
    if (!std.mem.startsWith(u8, remainder, ".u.")) return null;
    const parts_str = remainder[3..]; // Skip ".u."

    var parts = std.mem.splitSequence(u8, parts_str, ".");
    const index_name = parts.next() orelse return null;
    const generation_str = parts.next() orelse return null;
    if (parts.next() != null) return null; // Should be exactly 2 parts

    const generation = std.fmt.parseInt(u64, generation_str, 10) catch return null;
    return .{ .index_name = index_name, .generation = generation };
}

fn handleUpdateMessage(js_msg: *nats.JetStreamMessage, self: *Self) !void {
    defer js_msg.deinit();

    if (self.stopping.load(.acquire)) {
        log.debug("ignoring update message '{s}' while stopping", .{js_msg.msg.subject});
        return;
    }

    // Parse subject: {prefix}.u.{index}.{generation}
    const parsed = self.parseUpdatesSubject(js_msg.msg.subject) orelse {
        log.warn("invalid updates subject format: {s}", .{js_msg.msg.subject});
        return;
    };
    const index_name = parsed.index_name;
    const generation = parsed.generation;

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


    switch (meta_op) {
        .create => |create_op| { // create
            // Try to create the index with the NATS generation
            const create_result = self.local_indexes.createIndex(index_name, .{
                .generation = generation,
            }) catch |err| switch (err) {
                error.OlderIndexAlreadyExists => blk: {
                    // Reconcile: local index has older generation, delete and recreate
                    log.info("reconciling index {s}: local index is older, deleting and recreating", .{index_name});

                    // Stop any existing updater for this index
                    self.stopIndexUpdater(index_name) catch |stop_err| {
                        log.err("failed to stop updater for index {s}: {}", .{ index_name, stop_err });
                        return stop_err;
                    };

                    // Delete the local index to advance redirect.version
                    _ = self.local_indexes.deleteIndex(index_name, .{}) catch |delete_err| {
                        log.warn("failed to delete local index {s} for reconciliation: {}", .{ index_name, delete_err });
                        return delete_err;
                    };

                    log.debug("deleted local index {s} for reconciliation", .{index_name});

                    // Now create the index with the correct generation
                    const reconcile_result = self.local_indexes.createIndex(index_name, .{
                        .generation = generation,
                    }) catch |create_err| {
                        log.warn("failed to create local index {s} after reconciliation: {}", .{ index_name, create_err });
                        return create_err;
                    };

                    std.debug.assert(reconcile_result.generation == generation);
                    log.info("created local index {s} with generation {} after reconciliation", .{ index_name, generation });
                    break :blk reconcile_result;
                },
                error.NewerIndexAlreadyExists => {
                    // Local index is newer, this shouldn't happen - fail and let message be redelivered
                    log.err("local index {s} is newer than NATS generation {}, failing", .{ index_name, generation });
                    return err;
                },
                else => {
                    log.warn("failed to create local index {s}: {}", .{ index_name, err });
                    return err;
                },
            };

            // Assert that the result has the expected generation
            std.debug.assert(create_result.generation == generation);

            // Start updater for the index (idempotent) — fail to trigger redelivery/retry
            self.startIndexUpdater(index_name, generation, create_op.first_seq) catch |err| {
                log.err("failed to start updater for index {s}: {}", .{ index_name, err });
                return err;
            };
        },
        .delete => { // delete
            // Stop updater for the index
            self.stopIndexUpdater(index_name) catch |err| {
                log.err("failed to stop updater for index {s}: {}", .{ index_name, err });
                return err;
            };

            // Delete local index
            const delete_result = self.local_indexes.deleteIndex(index_name, .{}) catch |err| {
                log.warn("failed to delete local index {s}: {}", .{ index_name, err });
                return err;
            };

            if (delete_result.deleted) {
                log.info("deleted local index {s}", .{index_name});
            } else {
                log.debug("local index {s} already deleted", .{index_name});
            }
        },
    }
}

fn processUpdateOperation(self: *Self, index_name: []const u8, generation: u64, msg: *nats.JetStreamMessage) !void {
    const updater = self.getIndexUpdater(index_name, generation) orelse {
        log.warn("no updater found for index {s} generation {}", .{ index_name, generation });
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
        log.err("failed to apply update to local index {s}: {}", .{ index_name, err });
        return err;
    };

    // Update per-index last_applied_seq
    std.debug.assert(msg.metadata.sequence.stream > updater.last_applied_seq);
    updater.last_applied_seq = msg.metadata.sequence.stream;

    log.debug("applied update to local index {s} (seq={})", .{ index_name, msg.metadata.sequence.stream });
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

    const update_subject = try std.fmt.allocPrint(self.allocator, "{s}.u.{s}.{d}", .{ self.prefix, index_name, generation });
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
    index_name: []const u8,
    request: api.CreateIndexRequest,
) !api.CreateIndexResponse {
    if (!isValidIndexName(index_name)) {
        return error.InvalidIndexName;
    }

    // First check if index exists and get current generation
    const status = try self.getStatus(index_name);
    if (status.is_active) {
        if (request.expect_does_not_exist) {
            return error.IndexAlreadyExists;
        }
        const version = try self.getLastVersion(index_name, status.generation);
        return api.CreateIndexResponse{ .version = version, .ready = false, .generation = status.generation };
    }

    // Get the current last sequence from updates stream
    const updates_stream_info = self.js.getStreamInfo(self.updates_stream_name) catch |err| switch (err) {
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
    const subject = try std.fmt.allocPrint(self.allocator, "{s}.m.{s}", .{ self.prefix, index_name });
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

    // New index has no updates yet; advertise version 0 so clients can gate with expected_last_sequence=0.
    // Generation will be the NATS sequence number of this create operation
    return api.CreateIndexResponse{ .version = 0, .ready = false, .generation = result.value.seq };
}

pub fn deleteIndex(self: *Self, index_name: []const u8, request: api.DeleteIndexRequest) !api.DeleteIndexResponse {
    if (!isValidIndexName(index_name)) {
        return error.InvalidIndexName;
    }

    // Get current status and generation
    const status = try self.getStatus(index_name);
    if (!status.is_active) {
        if (request.expect_exists) {
            return error.IndexNotFound;
        }
        return api.DeleteIndexResponse{ .deleted = false }; // Already deleted
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

    const subject = try std.fmt.allocPrint(self.allocator, "{s}.m.{s}", .{ self.prefix, index_name });
    defer self.allocator.free(subject);

    const msg_id = try std.fmt.allocPrint(self.allocator, "delete-{s}-{d}", .{ index_name, generation });
    defer self.allocator.free(msg_id);

    const result = try self.js.publish(subject, data.items, .{ .msg_id = msg_id });
    defer result.deinit();

    return api.DeleteIndexResponse{ .deleted = true };
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
