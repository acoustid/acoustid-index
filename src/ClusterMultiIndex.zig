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

const META_INDEX_NAME = "_meta";

// Core state
allocator: std.mem.Allocator,
nc: *nats.Connection,
local_indexes: *MultiIndex,
replica_id: []const u8,

// State tracking
index_status: std.StringHashMapUnmanaged(IndexStatus) = .{}, // index_name -> status
lock: std.Thread.Mutex = .{},

// NATS JetStream
js: nats.JetStream = undefined,
main_consumer: ?*nats.PullSubscription = null,

// Stream configuration
stream_name: []const u8 = "fpindex-ops",

// Consumer thread
consumer_thread: ?std.Thread = null,
should_stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

/// Status of an index in the cluster
const IndexStatus = struct {
    generation: u64,
    last_applied_seq: u64,
};

/// Metadata operation (create/delete)
pub const MetaOp = union(enum) {
    create: struct {
        index_name: []const u8,
    },
    delete: struct {
        index_name: []const u8,
        generation: u64,
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
    };
}

fn loadOrCreateReplicaId(allocator: std.mem.Allocator, local_indexes: *MultiIndex) ![]const u8 {
    // Get or create the _meta index (local only, not replicated)
    const index = local_indexes.getOrCreateIndex(META_INDEX_NAME, true, null) catch |err| {
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
    var buf: [16]u8 = undefined;
    std.crypto.random.bytes(&buf);
    const new_id = try std.fmt.allocPrint(allocator, "{}", .{std.fmt.fmtSliceHexLower(&buf)});
    errdefer allocator.free(new_id);

    // Store the new replica_id in metadata
    var new_metadata = Metadata.initOwned(allocator);
    defer new_metadata.deinit();
    try new_metadata.set("cluster.replica_id", new_id);

    _ = index.update(&[_]Change{}, new_metadata, null) catch |err| {
        log.warn("failed to store replica_id in _meta index: {}", .{err});
        return err;
    };

    log.info("generated new replica_id: {s}", .{new_id});
    return try allocator.dupe(u8, new_id);
}

pub fn deinit(self: *Self) void {
    self.stop();

    self.lock.lock();
    defer self.lock.unlock();

    // Free map contents
    var status_iter = self.index_status.iterator();
    while (status_iter.next()) |entry| {
        self.allocator.free(entry.key_ptr.*);
    }
    self.index_status.deinit(self.allocator);

    self.allocator.free(self.replica_id);
}

pub fn start(self: *Self) !void {
    // Initialize JetStream
    self.js = nats.JetStream.init(self.nc, .{});

    // Ensure stream exists
    try self.ensureStream();

    // Load existing indexes and their last applied sequences
    try self.loadExistingIndexes();

    // Create main consumer
    try self.createMainConsumer();

    // Start consumer thread
    self.consumer_thread = try std.Thread.spawn(.{}, consumerLoop, .{self});
}

pub fn stop(self: *Self) void {
    if (self.should_stop.load(.acquire)) return;

    self.should_stop.store(true, .release);

    if (self.consumer_thread) |thread| {
        thread.join();
        self.consumer_thread = null;
    }

    if (self.main_consumer) |consumer| {
        consumer.deinit();
        self.main_consumer = null;
    }
}

fn getStatus(self: *Self, index_name: []const u8) !?u64 {
    const subject = try std.fmt.allocPrint(self.allocator, "fpindex.{s}.meta", .{index_name});
    defer self.allocator.free(subject);

    const msg = self.js.getMsg(self.stream_name, .{ .last_by_subj = subject, .direct = true }) catch |err| switch (err) {
        error.MessageNotFound => return null, // deleted or never existed
        else => return err,
    };
    defer msg.deinit();

    var result = try msgpack.decodeFromSlice(MetaOp, self.allocator, msg.data);
    defer result.deinit();

    const meta_op = result.value;

    switch (meta_op) {
        .create => return msg.seq, // generation from NATS sequence
        .delete => return null, // deleted
    }
}

fn getLastVersion(self: *Self, index_name: []const u8, generation: u64) !u64 {
    // Get the last sequence number for this index's updates
    const update_subject = try std.fmt.allocPrint(self.allocator, "fpindex.{s}.{d}", .{ index_name, generation });
    defer self.allocator.free(update_subject);

    const last_update_msg = self.js.getMsg(self.stream_name, .{ .last_by_subj = update_subject, .direct = true }) catch |err| switch (err) {
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
    const stream_config = nats.StreamConfig{
        .name = self.stream_name,
        .subjects = &[_][]const u8{"fpindex.>"},
        .retention = .limits,
        .storage = .file,
        .num_replicas = 1,
        .allow_direct = true,
        .allow_rollup_hdrs = true,
        .discard = .new,
    };

    var stream_info = self.js.addStream(stream_config) catch |err| switch (err) {
        error.StreamNameExist => return, // Already exists, ignore
        else => return err,
    };
    defer stream_info.deinit();
}

fn loadExistingIndexes(self: *Self) !void {
    self.lock.lock();
    defer self.lock.unlock();

    // Get list of existing indexes from MultiIndex
    const index_list = try self.local_indexes.listIndexes(self.allocator, .{});
    defer self.allocator.free(index_list);

    for (index_list) |info| {
        // Skip system indexes
        if (std.mem.eql(u8, info.name, META_INDEX_NAME)) {
            continue;
        }

        // Get the index to access its metadata
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

        var metadata = reader.getMetadata(self.allocator) catch |err| {
            log.warn("failed to get metadata for index {s} during startup: {}", .{ info.name, err });
            continue;
        };
        defer metadata.deinit();

        // Extract last applied sequence from cluster metadata
        const last_seq = if (metadata.get("cluster.last_applied_seq")) |seq_str|
            std.fmt.parseInt(u64, seq_str, 10) catch 0
        else
            0;

        // Use generation from cluster metadata if available, otherwise use redirect version
        const generation = if (metadata.get("cluster.generation")) |gen_str|
            std.fmt.parseInt(u64, gen_str, 10) catch info.generation
        else
            info.generation;

        // Store in our status map
        const status_entry = try self.index_status.getOrPut(self.allocator, info.name);
        if (!status_entry.found_existing) {
            status_entry.key_ptr.* = try self.allocator.dupe(u8, info.name);
        }
        status_entry.value_ptr.* = IndexStatus{
            .generation = generation,
            .last_applied_seq = last_seq,
        };

        log.info("loaded existing index {s} (generation={}, last_seq={})", .{ info.name, generation, last_seq });
    }
}

fn createMainConsumer(self: *Self) !void {
    const consumer_name = try std.fmt.allocPrint(self.allocator, "replica-{s}", .{self.replica_id});
    defer self.allocator.free(consumer_name);

    const consumer_config = nats.ConsumerConfig{
        .durable_name = consumer_name,
        .ack_policy = .explicit,
        .deliver_policy = .all,
        .filter_subject = "fpindex.>",
    };

    self.main_consumer = try self.js.pullSubscribe(self.stream_name, consumer_config);
}

fn consumerLoop(self: *Self) void {
    while (!self.should_stop.load(.acquire)) {
        self.processMessages() catch |err| {
            log.err("consumer loop error: {}", .{err});
            std.time.sleep(std.time.ns_per_s); // Wait before retry
        };
    }
}

fn processMessages(self: *Self) !void {
    const consumer = self.main_consumer orelse return;

    var batch = try consumer.fetch(10, 1000);
    defer batch.deinit();

    for (batch.messages) |msg| {
        defer msg.ack() catch |err| {
            log.err("failed to ack message: {}", .{err});
        };

        try self.processMessage(msg);
    }
}

fn processMessage(self: *Self, msg: *nats.JetStreamMessage) !void {
    const subject = msg.msg.subject;

    // Parse subject: fpindex.{index}.{type}
    if (!std.mem.startsWith(u8, subject, "fpindex.")) return;

    const parts_str = subject[8..]; // Remove "fpindex."
    var parts = std.mem.splitSequence(u8, parts_str, ".");
    const index_name = parts.next() orelse return;
    const operation_type = parts.next() orelse return;

    // Check if we should skip this message
    {
        self.lock.lock();
        defer self.lock.unlock();

        const status = self.index_status.get(index_name) orelse IndexStatus{ .generation = 0, .last_applied_seq = 0 };
        const last_applied = status.last_applied_seq;
        if (msg.metadata.sequence.stream <= last_applied) {
            log.debug("skipping already applied message for {s} (seq={}, last_applied={})", .{ index_name, msg.metadata.sequence.stream, last_applied });
            return;
        }
    }

    if (std.mem.eql(u8, operation_type, "meta")) {
        try self.processMetaOperation(index_name, msg);
    } else {
        // Parse generation from operation_type (should be a number)
        const generation = std.fmt.parseInt(u64, operation_type, 10) catch {
            log.warn("invalid generation in subject {s}", .{subject});
            return;
        };
        try self.processUpdateOperation(index_name, generation, msg);
    }
}

fn processMetaOperation(self: *Self, index_name: []const u8, msg: *nats.JetStreamMessage) !void {
    const meta_op_parsed = try msgpack.decodeFromSlice(MetaOp, self.allocator, msg.msg.data);
    defer meta_op_parsed.deinit();
    const meta_op = meta_op_parsed.value;

    const generation = msg.metadata.sequence.stream;

    self.lock.lock();
    defer self.lock.unlock();

    switch (meta_op) {
        .create => { // create
            try self.index_status.ensureUnusedCapacity(self.allocator, 1);

            const owned_index_name = try self.allocator.dupe(u8, index_name);
            errdefer self.allocator.free(owned_index_name);

            // Create the index locally with the NATS generation as the version
            _ = self.local_indexes.createIndexInternal(index_name, .{ .generation = generation }) catch |err| {
                log.warn("failed to create local index {s}: {}", .{ index_name, err });
                return err;
            };

            // Store generation and sequence tracking
            self.index_status.putAssumeCapacityNoClobber(owned_index_name, .{
                .generation = generation,
                .last_applied_seq = msg.metadata.sequence.stream,
            });

            // Update metadata in the index
            self.updateIndexMetadata(index_name, msg.metadata.sequence.stream, generation) catch |err| {
                log.warn("failed to update index metadata for {s}: {}", .{ index_name, err });
                // this is not critical, we can ignore it
            };

            log.info("created index {s} with generation {}", .{ index_name, generation });
        },
        .delete => |delete_op| { // delete
            // Delete local index with version validation and custom version from NATS sequence
            self.local_indexes.deleteIndexInternal(index_name, .{ .expected_generation = delete_op.generation, .generation = generation }) catch |err| {
                log.warn("failed to delete local index {s}: {}", .{ index_name, err });
                return err;
            };

            // Remove from status map
            if (self.index_status.fetchRemove(index_name)) |entry| {
                self.allocator.free(entry.key);
            }

            log.info("deleted index {s} with generation {}", .{ index_name, generation });
        },
    }
}

fn processUpdateOperation(self: *Self, index_name: []const u8, generation: u64, msg: *nats.JetStreamMessage) !void {
    self.lock.lock();
    defer self.lock.unlock();

    // Check if index exists and validate generation
    var status = self.index_status.getPtr(index_name) orelse {
        log.warn("update for non-existent index {s}", .{index_name});
        return;
    };

    if (generation != status.generation) {
        log.warn("update for index {s} with wrong generation {} (expected {})", .{ index_name, generation, status.generation });
        return;
    }

    // Decode the update operation
    var update_op = try msgpack.decodeFromSlice(UpdateOp, self.allocator, msg.msg.data);
    defer update_op.deinit();

    // Inject sequence tracking metadata field
    var metadata = update_op.value.metadata orelse Metadata.initOwned(update_op.arena.allocator());
    try injectIndexMetadata(&metadata, msg.metadata.sequence.stream, null);

    // Apply the update to local index
    const update_request = api.UpdateRequest{
        .changes = update_op.value.changes,
        .metadata = metadata,
        .expected_version = null, // Version control handled at NATS level
    };

    _ = self.local_indexes.update(self.allocator, index_name, update_request) catch |err| {
        log.err("failed to apply update to index {s}: {}", .{ index_name, err });
        return;
    };

    // Update local sequence tracking
    status.last_applied_seq = msg.metadata.sequence.stream;

    log.debug("applied update to index {s} (seq={})", .{ index_name, msg.metadata.sequence.stream });
}

fn injectIndexMetadata(metadata: *Metadata, sequence: u64, generation: ?u64) !void {
    var buf: [32]u8 = undefined;
    try metadata.set("cluster.last_applied_seq", try std.fmt.bufPrint(&buf, "{d}", .{sequence}));
    if (generation) |gen| {
        try metadata.set("cluster.generation", try std.fmt.bufPrint(&buf, "{d}", .{gen}));
    }
}

fn updateIndexMetadata(self: *Self, index_name: []const u8, sequence: u64, generation: ?u64) !void {
    // Get the index
    const index = self.local_indexes.getIndex(index_name) catch return;
    defer self.local_indexes.releaseIndex(index);

    // Create metadata update
    var metadata = Metadata.initOwned(self.allocator);
    defer metadata.deinit();

    try injectIndexMetadata(&metadata, sequence, generation);

    // Apply metadata update
    _ = index.update(&[_]Change{}, metadata, null) catch |err| {
        log.warn("failed to update metadata for index {s}: {}", .{ index_name, err });
        return err;
    };
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

    // First check if index exists and get current generation
    const generation = try self.getStatus(index_name) orelse {
        return error.IndexNotFound;
    };

    // Prepare update operation
    const update_op = UpdateOp{
        .changes = request.changes,
        .metadata = request.metadata,
    };

    var data = std.ArrayList(u8).init(self.allocator);
    defer data.deinit();

    try msgpack.encode(update_op, data.writer());

    const update_subject = try std.fmt.allocPrint(self.allocator, "fpindex.{s}.{d}", .{ index_name, generation });
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
    const response = try self.local_indexes.getIndexInfo(allocator, index_name);

    // Use cluster metadata from the index for consistent version info
    const version = if (response.metadata.get("cluster.last_applied_seq")) |seq_str|
        std.fmt.parseInt(u64, seq_str, 10) catch response.version
    else
        response.version;

    return api.GetIndexInfoResponse{
        .version = version,
        .metadata = response.metadata,
        .stats = response.stats,
    };
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

    // First check if index exists and get current generation
    if (try self.getStatus(index_name)) |generation| {
        const version = try self.getLastVersion(index_name, generation);
        return api.CreateIndexResponse{ .version = version };
    }

    // Check current status
    const subject = try std.fmt.allocPrint(self.allocator, "fpindex.{s}.meta", .{index_name});
    defer self.allocator.free(subject);

    // Publish create operation
    const meta_op = MetaOp{
        .create = .{
            .index_name = index_name,
        },
    };

    var data = std.ArrayList(u8).init(self.allocator);
    defer data.deinit();

    try msgpack.encode(meta_op, data.writer());

    const result = try self.js.publish(subject, data.items, .{});
    defer result.deinit();

    return api.CreateIndexResponse{ .version = result.value.seq };
}

pub fn deleteIndex(self: *Self, index_name: []const u8) !void {
    // Get current status and generation
    const generation = try self.getStatus(index_name) orelse {
        return; // Already deleted
    };

    // Publish delete operation
    const meta_op = MetaOp{
        .delete = .{
            .index_name = index_name,
            .generation = generation,
        },
    };

    var data = std.ArrayList(u8).init(self.allocator);
    defer data.deinit();

    try msgpack.encode(meta_op, data.writer());

    const subject = try std.fmt.allocPrint(self.allocator, "fpindex.{s}.meta", .{index_name});
    defer self.allocator.free(subject);

    const result = try self.js.publish(subject, data.items, .{});
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
