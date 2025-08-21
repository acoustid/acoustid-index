const std = @import("std");
const Message = @import("message.zig").Message;
const errors = @import("errors.zig");

/// Callback function type for handling messages
pub const MessageCallback = *const fn (msg: *Message) void;

/// Basic subscription for core NATS
pub const Subscription = struct {
    /// Subscription ID
    sid: u64,
    /// Subject pattern
    subject: []const u8,
    /// Queue group (optional)
    queue: ?[]const u8,
    /// Message callback
    callback: ?MessageCallback,
    /// Maximum messages to receive (for auto-unsubscribe)
    max_msgs: ?u64,
    /// Number of messages received so far
    msgs_received: u64,
    /// Whether subscription is active
    active: bool,
    
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, sid: u64, subject: []const u8, queue: ?[]const u8, callback: ?MessageCallback) !Subscription {
        return Subscription{
            .sid = sid,
            .subject = try allocator.dupe(u8, subject),
            .queue = if (queue) |q| try allocator.dupe(u8, q) else null,
            .callback = callback,
            .max_msgs = null,
            .msgs_received = 0,
            .active = true,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Subscription) void {
        self.allocator.free(self.subject);
        if (self.queue) |q| {
            self.allocator.free(q);
        }
    }

    pub fn handleMessage(self: *Subscription, message: *Message) void {
        if (!self.active) return;
        
        self.msgs_received += 1;
        
        if (self.callback) |cb| {
            cb(message);
        }
        
        // Auto-unsubscribe if max messages reached
        if (self.max_msgs) |max| {
            if (self.msgs_received >= max) {
                self.active = false;
            }
        }
    }

    pub fn setMaxMessages(self: *Subscription, max_msgs: u64) void {
        self.max_msgs = max_msgs;
    }

    pub fn unsubscribe(self: *Subscription) void {
        self.active = false;
    }
};

/// JetStream pull subscription
pub const PullSubscription = struct {
    /// Base subscription
    base: Subscription,
    /// Stream name
    stream_name: []const u8,
    /// Consumer name
    consumer_name: []const u8,
    /// Durable consumer name (optional)
    durable_name: ?[]const u8,
    
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, sid: u64, subject: []const u8, stream_name: []const u8, consumer_name: []const u8, durable_name: ?[]const u8) !PullSubscription {
        const base = try Subscription.init(allocator, sid, subject, null, null);
        return PullSubscription{
            .base = base,
            .stream_name = try allocator.dupe(u8, stream_name),
            .consumer_name = try allocator.dupe(u8, consumer_name),
            .durable_name = if (durable_name) |dn| try allocator.dupe(u8, dn) else null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *PullSubscription) void {
        self.base.deinit();
        self.allocator.free(self.stream_name);
        self.allocator.free(self.consumer_name);
        if (self.durable_name) |dn| {
            self.allocator.free(dn);
        }
    }
};

/// JetStream push subscription
pub const PushSubscription = struct {
    /// Base subscription
    base: Subscription,
    /// Stream name
    stream_name: []const u8,
    /// Consumer configuration
    consumer_name: []const u8,
    /// Durable consumer name (optional)
    durable_name: ?[]const u8,
    
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, sid: u64, subject: []const u8, stream_name: []const u8, consumer_name: []const u8, durable_name: ?[]const u8, callback: ?MessageCallback) !PushSubscription {
        const base = try Subscription.init(allocator, sid, subject, null, callback);
        return PushSubscription{
            .base = base,
            .stream_name = try allocator.dupe(u8, stream_name),
            .consumer_name = try allocator.dupe(u8, consumer_name),
            .durable_name = if (durable_name) |dn| try allocator.dupe(u8, dn) else null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *PushSubscription) void {
        self.base.deinit();
        self.allocator.free(self.stream_name);
        self.allocator.free(self.consumer_name);
        if (self.durable_name) |dn| {
            self.allocator.free(dn);
        }
    }
};