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

/// Message types for operations
pub const OpType = enum(u8) {
    create = 1,
    delete = 2,
};

/// Status of an index in the cluster
const IndexStatus = struct {
    generation: u64,
    last_applied_seq: u64,
};

/// Metadata operation (create/delete)
pub const MetaOp = struct {
    op: u8, // 1 = create, 2 = delete
    index_name: []const u8,
    generation: u64 = 0, // set for delete (required), 0 for create (auto-generated)

    pub fn msgpackFormat() msgpack.StructFormat {
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

pub fn init(allocator: std.mem.Allocator, nc: *nats.Connection, local_indexes: *MultiIndex) Self {
    return .{
        .allocator = allocator,
        .nc = nc,
        .local_indexes = local_indexes,
        .replica_id = generateReplicaId(allocator),
    };
}

fn generateReplicaId(allocator: std.mem.Allocator) []const u8 {
    var buf: [16]u8 = undefined;
    std.crypto.random.bytes(&buf);
    return std.fmt.allocPrint(allocator, "{}", .{std.fmt.fmtSliceHexLower(&buf)}) catch unreachable;
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

fn getLastMetaOp(self: *Self, index_name: []const u8) !MetaOp {
    const subject = try std.fmt.allocPrint(self.allocator, "fpindex.{s}.meta", .{index_name});
    defer self.allocator.free(subject);

    const msg = self.js.getMsg(self.stream_name, .{ .last_by_subj = subject, .direct = true }) catch |err| switch (err) {
        error.MessageNotFound => return .{ .index_name = index_name, .op = 2 },
        else => return err,
    };
    defer msg.deinit();

    var result = try msgpack.decodeFromSlice(MetaOp, self.allocator, msg.data);
    defer result.deinit();

    var meta_op = result.value;

    // Use the original index_name, since the caller owns that and we can free our copy
    std.debug.assert(std.mem.eql(u8, meta_op.index_name, index_name));
    meta_op.index_name = index_name;

    // For create operations (op == 1), set generation to message sequence if not already set
    if (meta_op.op == 1) {
        meta_op.generation = msg.seq;
    }

    return meta_op;
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

    // Iterate over existing index directories
    var iter = self.local_indexes.dir.iterate();
    while (try iter.next()) |entry| {
        if (entry.kind != .directory) continue;
        // TODO: We should validate the name, but isValidName is private
        // For now, assume all directory names are valid indexes

        // Try to read redirect file
        var index_dir = self.local_indexes.dir.openDir(entry.name, .{}) catch continue;
        defer index_dir.close();

        const redirect = index_redirect.readRedirectFile(index_dir, self.allocator) catch continue;
        defer self.allocator.free(redirect.name);

        if (redirect.deleted) continue;

        // Try to get the index
        const index = self.local_indexes.getIndex(entry.name) catch continue;
        defer self.local_indexes.releaseIndex(index);

        var reader = index.acquireReader() catch continue;
        defer index.releaseReader(&reader);

        var metadata = reader.getMetadata(self.allocator) catch continue;
        defer metadata.deinit();

        // Extract last applied sequence and generation from metadata
        const last_seq = if (metadata.get("cluster.last_applied_seq")) |seq_str|
            std.fmt.parseInt(u64, seq_str, 10) catch 0
        else
            0;

        const generation = if (metadata.get("cluster.generation")) |gen_str|
            std.fmt.parseInt(u32, gen_str, 10) catch @as(u32, @intCast(redirect.version))
        else
            @as(u32, @intCast(redirect.version));

        // Store in our status map
        const status_entry = try self.index_status.getOrPut(self.allocator, entry.name);
        if (!status_entry.found_existing) {
            status_entry.key_ptr.* = try self.allocator.dupe(u8, entry.name);
        }
        status_entry.value_ptr.* = IndexStatus{
            .generation = generation,
            .last_applied_seq = last_seq,
        };

        log.info("loaded existing index {s} (generation={}, last_seq={})", .{ entry.name, generation, last_seq });
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

    self.lock.lock();
    defer self.lock.unlock();

    switch (meta_op.op) {
        1 => { // create
            const generation = @as(u32, @intCast(msg.metadata.sequence.stream));

            // Note: We create the index with standard API, not with custom redirect
            // TODO: Ideally we want IndexRedirect.version = generation for debugging

            // Create the index locally using standard API
            _ = self.local_indexes.createIndex(self.allocator, index_name) catch |err| {
                log.warn("failed to create local index {s}: {}", .{ index_name, err });
                return;
            };

            // TODO: We need to update the IndexRedirect version after creation to match NATS generation
            // For now, this creates with version 1, but ideally we want version = generation

            // Store generation and sequence tracking
            const status_entry = try self.index_status.getOrPut(self.allocator, index_name);
            if (!status_entry.found_existing) {
                status_entry.key_ptr.* = try self.allocator.dupe(u8, index_name);
            }
            status_entry.value_ptr.* = IndexStatus{
                .generation = generation,
                .last_applied_seq = msg.metadata.sequence.stream,
            };

            // Update metadata in the index
            try self.updateIndexMetadata(index_name, msg.metadata.sequence.stream, generation);

            log.info("created index {s} with generation {}", .{ index_name, generation });
        },
        2 => { // delete
            if (self.index_status.get(index_name)) |status| {
                const delete_gen = meta_op.generation;
                if (status.generation == delete_gen) {
                    // Delete local index
                    self.local_indexes.deleteIndex(index_name) catch |err| {
                        log.warn("failed to delete local index {s}: {}", .{ index_name, err });
                    };

                    // Remove from status map
                    if (self.index_status.fetchRemove(index_name)) |entry| {
                        self.allocator.free(entry.key);
                    }

                    log.info("deleted index {s}", .{index_name});
                }
            }
        },
        else => {
            log.warn("unknown operation type: {}", .{meta_op.op});
        },
    }
}

fn processUpdateOperation(self: *Self, index_name: []const u8, generation: u64, msg: *nats.JetStreamMessage) !void {
    self.lock.lock();
    defer self.lock.unlock();

    // Check if index exists and validate generation
    const status = self.index_status.get(index_name) orelse {
        log.warn("update for non-existent index {s}", .{index_name});
        return;
    };

    if (generation != status.generation) {
        log.warn("update for index {s} with wrong generation {} (expected {})", .{ index_name, generation, status.generation });
        return;
    }

    // Decode the update operation
    const update_op_parsed = try msgpack.decodeFromSlice(UpdateOp, self.allocator, msg.msg.data);
    defer update_op_parsed.deinit();
    const update_op = update_op_parsed.value;

    // Apply the update to local index
    const update_request = api.UpdateRequest{
        .changes = @constCast(update_op.changes),
        .metadata = update_op.metadata,
        .expected_version = null, // Version control handled at NATS level
    };

    _ = self.local_indexes.update(self.allocator, index_name, update_request) catch |err| {
        log.err("failed to apply update to index {s}: {}", .{ index_name, err });
        return;
    };

    // Update sequence tracking
    const status_entry = try self.index_status.getOrPut(self.allocator, index_name);
    if (!status_entry.found_existing) {
        status_entry.key_ptr.* = try self.allocator.dupe(u8, index_name);
    }
    status_entry.value_ptr.last_applied_seq = msg.metadata.sequence.stream;

    // Update metadata with new sequence
    try self.updateIndexMetadata(index_name, msg.metadata.sequence.stream, null);

    log.debug("applied update to index {s} (seq={})", .{ index_name, msg.metadata.sequence.stream });
}

fn updateIndexMetadata(self: *Self, index_name: []const u8, sequence: u64, generation: ?u64) !void {
    // Get the index
    const index = self.local_indexes.getIndex(index_name) catch return;
    defer self.local_indexes.releaseIndex(index);

    // Create metadata update
    var metadata = Metadata.initOwned(self.allocator);
    errdefer metadata.deinit();

    try metadata.set("cluster.last_applied_seq", try std.fmt.allocPrint(self.allocator, "{}", .{sequence}));

    if (generation) |gen| {
        try metadata.set("cluster.generation", try std.fmt.allocPrint(self.allocator, "{}", .{gen}));
    }

    // Apply metadata update
    _ = index.update(&[_]Change{}, metadata, null) catch |err| {
        log.warn("failed to update metadata for index {s}: {}", .{ index_name, err });
        metadata.deinit();
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
    const status = try self.getLastMetaOp(index_name);
    if (status.op == 2) {
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

    const update_subject = try std.fmt.allocPrint(self.allocator, "fpindex.{s}.{d}", .{ index_name, generation });
    defer self.allocator.free(update_subject);

    var publish_opts = nats.PublishOptions{};
    if (request.expected_version) |expected_version| {
        publish_opts.expected_last_subject_seq = expected_version;
    }

    const result = try self.js.publish(update_subject, data.items, publish_opts);
    defer result.deinit();

    return api.UpdateResponse{ .version = result.value.seq };
}

pub fn getIndexInfo(
    self: *Self,
    allocator: std.mem.Allocator,
    index_name: []const u8,
) !api.GetIndexInfoResponse {
    const response = try self.local_indexes.getIndexInfo(allocator, index_name);

    // Override version with last applied sequence for clustered version
    self.lock.lock();
    defer self.lock.unlock();

    if (self.index_status.get(index_name)) |status| {
        return api.GetIndexInfoResponse{
            .version = status.last_applied_seq,
            .metadata = response.metadata,
            .stats = response.stats,
        };
    }

    return response;
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
    const status = try self.getLastMetaOp(index_name);
    if (status.op == 1) { // create
        // FIXME version should be the last seq from fpindex.{idx}.{gen}
        return api.CreateIndexResponse{ .version = status.generation };
    }

    // Check current status
    const subject = try std.fmt.allocPrint(self.allocator, "fpindex.{s}.meta", .{index_name});
    defer self.allocator.free(subject);

    // Publish create operation
    const meta_op = MetaOp{
        .op = 1, // create
        .index_name = index_name,
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
    const status = try self.getLastMetaOp(index_name);
    if (status.op == 2) { // delete
        return; // Already deleted
    }

    // Publish delete operation
    const meta_op = MetaOp{
        .op = 2, // delete
        .index_name = index_name,
        .generation = status.generation,
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
