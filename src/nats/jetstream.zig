const std = @import("std");
const Allocator = std.mem.Allocator;

const errors = @import("errors.zig");
const Message = @import("message.zig").Message;
const PullSubscription = @import("subscription.zig").PullSubscription;
const PushSubscription = @import("subscription.zig").PushSubscription;

// Forward declaration
const Client = @import("client.zig").Client;

/// JetStream retention policy
pub const RetentionPolicy = enum {
    LIMITS,
    INTEREST,
    WORK_QUEUE,
    
    pub fn toString(self: RetentionPolicy) []const u8 {
        return switch (self) {
            .LIMITS => "limits",
            .INTEREST => "interest", 
            .WORK_QUEUE => "workqueue",
        };
    }
};

/// JetStream storage type
pub const StorageType = enum {
    FILE,
    MEMORY,
    
    pub fn toString(self: StorageType) []const u8 {
        return switch (self) {
            .FILE => "file",
            .MEMORY => "memory",
        };
    }
};

/// JetStream delivery policy
pub const DeliverPolicy = enum {
    ALL,
    LAST,
    NEW,
    BY_START_SEQUENCE,
    BY_START_TIME,
    LAST_PER_SUBJECT,
    
    pub fn toString(self: DeliverPolicy) []const u8 {
        return switch (self) {
            .ALL => "all",
            .LAST => "last",
            .NEW => "new",
            .BY_START_SEQUENCE => "by_start_sequence",
            .BY_START_TIME => "by_start_time",
            .LAST_PER_SUBJECT => "last_per_subject",
        };
    }
};

/// JetStream acknowledgment policy
pub const AckPolicy = enum {
    NONE,
    ALL,
    EXPLICIT,
    
    pub fn toString(self: AckPolicy) []const u8 {
        return switch (self) {
            .NONE => "none",
            .ALL => "all",
            .EXPLICIT => "explicit",
        };
    }
};

/// Stream configuration
pub const StreamConfig = struct {
    name: []const u8,
    subjects: [][]const u8,
    retention: RetentionPolicy = .LIMITS,
    max_consumers: ?u32 = null,
    max_msgs: ?u64 = null,
    max_bytes: ?u64 = null,
    max_age: ?u64 = null, // nanoseconds
    max_msg_size: ?u32 = null,
    storage: StorageType = .FILE,
    num_replicas: ?u8 = null,
    duplicate_window: ?u64 = null, // nanoseconds

    pub fn toJson(self: StreamConfig, allocator: Allocator) ![]u8 {
        var json = std.ArrayList(u8).init(allocator);
        defer json.deinit();
        
        try json.appendSlice("{");
        try std.fmt.format(json.writer(), "\"name\":\"{s}\"", .{self.name});
        
        // Subjects array
        try json.appendSlice(",\"subjects\":[");
        for (self.subjects, 0..) |subject, i| {
            if (i > 0) try json.appendSlice(",");
            try std.fmt.format(json.writer(), "\"{s}\"", .{subject});
        }
        try json.appendSlice("]");
        
        try std.fmt.format(json.writer(), ",\"retention\":\"{s}\"", .{self.retention.toString()});
        try std.fmt.format(json.writer(), ",\"storage\":\"{s}\"", .{self.storage.toString()});
        
        if (self.max_consumers) |max| {
            try std.fmt.format(json.writer(), ",\"max_consumers\":{d}", .{max});
        }
        if (self.max_msgs) |max| {
            try std.fmt.format(json.writer(), ",\"max_msgs\":{d}", .{max});
        }
        if (self.max_bytes) |max| {
            try std.fmt.format(json.writer(), ",\"max_bytes\":{d}", .{max});
        }
        if (self.max_age) |max| {
            try std.fmt.format(json.writer(), ",\"max_age\":{d}", .{max});
        }
        if (self.max_msg_size) |max| {
            try std.fmt.format(json.writer(), ",\"max_msg_size\":{d}", .{max});
        }
        if (self.num_replicas) |num| {
            try std.fmt.format(json.writer(), ",\"num_replicas\":{d}", .{num});
        }
        if (self.duplicate_window) |window| {
            try std.fmt.format(json.writer(), ",\"duplicate_window\":{d}", .{window});
        }
        
        try json.appendSlice("}");
        return json.toOwnedSlice();
    }
};

/// Consumer configuration
pub const ConsumerConfig = struct {
    durable_name: ?[]const u8 = null,
    deliver_policy: DeliverPolicy = .ALL,
    opt_start_seq: ?u64 = null,
    opt_start_time: ?i64 = null,
    ack_policy: AckPolicy = .EXPLICIT,
    ack_wait: ?u64 = null, // nanoseconds
    max_deliver: ?u32 = null,
    filter_subject: ?[]const u8 = null,
    replay_policy: ?[]const u8 = null,
    rate_limit_bps: ?u64 = null,
    sample_freq: ?[]const u8 = null,
    max_waiting: ?u32 = null,
    max_ack_pending: ?u32 = null,
    flow_control: bool = false,
    idle_heartbeat: ?u64 = null, // nanoseconds

    pub fn toJson(self: ConsumerConfig, allocator: Allocator) ![]u8 {
        var json = std.ArrayList(u8).init(allocator);
        defer json.deinit();
        
        try json.appendSlice("{");
        
        var first = true;
        
        if (self.durable_name) |name| {
            try std.fmt.format(json.writer(), "\"durable_name\":\"{s}\"", .{name});
            first = false;
        }
        
        if (!first) try json.appendSlice(",");
        try std.fmt.format(json.writer(), "\"deliver_policy\":\"{s}\"", .{self.deliver_policy.toString()});
        first = false;
        
        if (!first) try json.appendSlice(",");
        try std.fmt.format(json.writer(), "\"ack_policy\":\"{s}\"", .{self.ack_policy.toString()});
        
        if (self.opt_start_seq) |seq| {
            try std.fmt.format(json.writer(), ",\"opt_start_seq\":{d}", .{seq});
        }
        if (self.opt_start_time) |time| {
            try std.fmt.format(json.writer(), ",\"opt_start_time\":{d}", .{time});
        }
        if (self.ack_wait) |wait| {
            try std.fmt.format(json.writer(), ",\"ack_wait\":{d}", .{wait});
        }
        if (self.max_deliver) |max| {
            try std.fmt.format(json.writer(), ",\"max_deliver\":{d}", .{max});
        }
        if (self.filter_subject) |filter| {
            try std.fmt.format(json.writer(), ",\"filter_subject\":\"{s}\"", .{filter});
        }
        if (self.rate_limit_bps) |rate| {
            try std.fmt.format(json.writer(), ",\"rate_limit_bps\":{d}", .{rate});
        }
        if (self.max_waiting) |max| {
            try std.fmt.format(json.writer(), ",\"max_waiting\":{d}", .{max});
        }
        if (self.max_ack_pending) |max| {
            try std.fmt.format(json.writer(), ",\"max_ack_pending\":{d}", .{max});
        }
        if (self.flow_control) {
            try std.fmt.format(json.writer(), ",\"flow_control\":true", .{});
        }
        if (self.idle_heartbeat) |heartbeat| {
            try std.fmt.format(json.writer(), ",\"idle_heartbeat\":{d}", .{heartbeat});
        }
        
        try json.appendSlice("}");
        return json.toOwnedSlice();
    }
};

/// JetStream publish acknowledgment
pub const PubAck = struct {
    stream: []const u8,
    seq: u64,
    duplicate: bool,
    
    allocator: Allocator,
    
    pub fn deinit(self: *PubAck) void {
        self.allocator.free(self.stream);
    }
};

/// JetStream context
pub const JetStream = struct {
    allocator: Allocator,
    client: *Client,
    prefix: []const u8,

    pub fn init(allocator: Allocator, client: *Client) !JetStream {
        return JetStream{
            .allocator = allocator,
            .client = client,
            .prefix = try allocator.dupe(u8, "$JS.API"),
        };
    }

    pub fn deinit(self: *JetStream) void {
        self.allocator.free(self.prefix);
    }

    /// Add or update a stream
    pub fn addStream(self: *JetStream, config: StreamConfig) !void {
        const subject = try std.fmt.allocPrint(self.allocator, "{s}.STREAM.CREATE.{s}", .{ self.prefix, config.name });
        defer self.allocator.free(subject);
        
        const config_json = try config.toJson(self.allocator);
        defer self.allocator.free(config_json);
        
        try self.client.publish(subject, null, config_json);
        
        // TODO: Wait for and parse response to check for errors
    }

    /// Delete a stream
    pub fn deleteStream(self: *JetStream, stream_name: []const u8) !void {
        const subject = try std.fmt.allocPrint(self.allocator, "{s}.STREAM.DELETE.{s}", .{ self.prefix, stream_name });
        defer self.allocator.free(subject);
        
        try self.client.publish(subject, null, "{}");
        
        // TODO: Wait for and parse response
    }

    /// Publish to JetStream with acknowledgment
    pub fn publish(self: *JetStream, subject: []const u8, payload: []const u8, headers: ?std.StringHashMap([]const u8)) !PubAck {
        _ = headers; // TODO: Handle JetStream headers
        // For now, use basic publish - in a full implementation,
        // this would handle JetStream-specific headers and wait for ack
        try self.client.publish(subject, null, payload);
        
        // Return a mock ack - in reality this would be parsed from the response
        return PubAck{
            .stream = try self.allocator.dupe(u8, "unknown"),
            .seq = 1,
            .duplicate = false,
            .allocator = self.allocator,
        };
    }

    /// Create a pull subscription
    pub fn pullSubscribe(self: *JetStream, subject: []const u8, stream_name: []const u8, consumer_config: ConsumerConfig) !*PullSubscription {
        // Create consumer first
        try self.addConsumer(stream_name, consumer_config);
        
        // Create pull subscription
        const subscription = try self.allocator.create(PullSubscription);
        const sid = self.client.next_sid;
        self.client.next_sid += 1;
        
        const consumer_name = consumer_config.durable_name orelse "ephemeral";
        
        subscription.* = try PullSubscription.init(
            self.allocator,
            sid,
            subject,
            stream_name,
            consumer_name,
            consumer_config.durable_name,
        );
        
        return subscription;
    }

    /// Create a push subscription
    pub fn pushSubscribe(self: *JetStream, subject: []const u8, stream_name: []const u8, consumer_config: ConsumerConfig) !*PushSubscription {
        // Create consumer first
        try self.addConsumer(stream_name, consumer_config);
        
        // Subscribe using regular NATS subscription
        const base_sub = try self.client.subscribe(subject, null);
        
        const subscription = try self.allocator.create(PushSubscription);
        const consumer_name = consumer_config.durable_name orelse "ephemeral";
        
        subscription.* = try PushSubscription.init(
            self.allocator,
            base_sub.sid,
            subject,
            stream_name,
            consumer_name,
            consumer_config.durable_name,
            null,
        );
        
        return subscription;
    }

    /// Pull messages from a pull subscription
    pub fn fetch(self: *JetStream, subscription: *PullSubscription, batch_size: u32, timeout_ms: u64) ![]Message {
        _ = timeout_ms; // TODO: Implement timeout handling
        // Send pull request
        const pull_subject = try std.fmt.allocPrint(self.allocator, "$JS.API.CONSUMER.MSG.NEXT.{s}.{s}", .{ subscription.stream_name, subscription.consumer_name });
        defer self.allocator.free(pull_subject);
        
        const pull_request = try std.fmt.allocPrint(self.allocator, "{{\"batch\":{d},\"no_wait\":true}}", .{batch_size});
        defer self.allocator.free(pull_request);
        
        try self.client.publish(pull_subject, null, pull_request);
        
        // TODO: Collect messages with timeout - for now return empty array
        const messages = try self.allocator.alloc(Message, 0);
        return messages;
    }

    // Private helper methods

    fn addConsumer(self: *JetStream, stream_name: []const u8, config: ConsumerConfig) !void {
        const subject = try std.fmt.allocPrint(self.allocator, "{s}.CONSUMER.CREATE.{s}", .{ self.prefix, stream_name });
        defer self.allocator.free(subject);
        
        const config_json = try config.toJson(self.allocator);
        defer self.allocator.free(config_json);
        
        try self.client.publish(subject, null, config_json);
        
        // TODO: Wait for and parse response
    }
};