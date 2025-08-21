const std = @import("std");

/// Message metadata for JetStream messages
pub const MessageMetadata = struct {
    /// Stream sequence number
    sequence: u64,
    /// Consumer sequence number
    consumer_sequence: u64,
    /// Subject
    subject: []const u8,
    /// Reply subject for ack/nak
    reply: ?[]const u8,
    /// Message timestamp
    timestamp: i64,
    /// Number of delivery attempts
    delivered: u64,

    pub fn init(sequence: u64, consumer_sequence: u64, subject: []const u8, reply: ?[]const u8, timestamp: i64, delivered: u64) MessageMetadata {
        return MessageMetadata{
            .sequence = sequence,
            .consumer_sequence = consumer_sequence,
            .subject = subject,
            .reply = reply,
            .timestamp = timestamp,
            .delivered = delivered,
        };
    }
};

/// NATS message
pub const Message = struct {
    /// Message subject
    subject: []const u8,
    /// Reply subject
    reply: ?[]const u8,
    /// Message data
    data: []const u8,
    /// JetStream metadata (null for core NATS)
    metadata: ?MessageMetadata,
    /// Headers (simplified - just store as key-value pairs)
    headers: std.StringHashMap([]const u8),
    
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, subject: []const u8, reply: ?[]const u8, data: []const u8) !Message {
        const headers = std.StringHashMap([]const u8).init(allocator);
        return Message{
            .subject = try allocator.dupe(u8, subject),
            .reply = if (reply) |r| try allocator.dupe(u8, r) else null,
            .data = try allocator.dupe(u8, data),
            .metadata = null,
            .headers = headers,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Message) void {
        self.allocator.free(self.subject);
        if (self.reply) |reply| {
            self.allocator.free(reply);
        }
        self.allocator.free(self.data);
        
        var iterator = self.headers.iterator();
        while (iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.headers.deinit();
    }
};