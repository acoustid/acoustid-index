const std = @import("std");
const net = std.net;
const Allocator = std.mem.Allocator;

const protocol = @import("protocol.zig");
const errors = @import("errors.zig");
const Message = @import("message.zig").Message;
const Subscription = @import("subscription.zig").Subscription;
const JetStream = @import("jetstream.zig").JetStream;

const ConnectInfo = protocol.ConnectInfo;
const ServerInfo = protocol.ServerInfo;
const Protocol = protocol.Protocol;
const MsgProtocol = protocol.MsgProtocol;
const Operation = protocol.Operation;

/// NATS client connection state
const ConnectionState = enum {
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
    CLOSED,
};

/// NATS client
pub const Client = struct {
    allocator: Allocator,
    stream: ?net.Stream,
    state: ConnectionState,
    
    /// Server information
    server_info: ?ServerInfo,
    
    /// Subscription management
    subscriptions: std.AutoHashMap(u64, *Subscription),
    next_sid: u64,
    
    /// Read buffer for incoming messages
    read_buffer: []u8,
    read_pos: usize,
    
    /// Connection configuration
    connect_info: ConnectInfo,
    
    /// JetStream context (lazy initialized)
    jetstream_ctx: ?*JetStream,

    const READ_BUFFER_SIZE = 32 * 1024; // 32KB read buffer

    pub fn init(allocator: Allocator) !*Client {
        const read_buffer = try allocator.alloc(u8, READ_BUFFER_SIZE);
        const client = try allocator.create(Client);
        
        client.* = Client{
            .allocator = allocator,
            .stream = null,
            .state = .DISCONNECTED,
            .server_info = null,
            .subscriptions = std.AutoHashMap(u64, *Subscription).init(allocator),
            .next_sid = 1,
            .read_buffer = read_buffer,
            .read_pos = 0,
            .connect_info = ConnectInfo{},
            .jetstream_ctx = null,
        };
        
        return client;
    }

    pub fn deinit(self: *Client) void {
        if (self.stream) |*stream| {
            stream.close();
        }
        
        if (self.server_info) |*info| {
            info.deinit();
        }
        
        // Clean up subscriptions
        var iterator = self.subscriptions.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.subscriptions.deinit();
        
        if (self.jetstream_ctx) |js| {
            js.deinit();
            self.allocator.destroy(js);
        }
        
        self.allocator.free(self.read_buffer);
        self.allocator.destroy(self);
    }

    /// Connect to NATS server
    pub fn connect(self: *Client, address: net.Address, timeout_ms: u64) !void {
        if (self.state != .DISCONNECTED) {
            return errors.Error.ConnectionFailed;
        }
        
        self.state = .CONNECTING;
        
        // Connect to server with timeout
        const stream = try net.tcpConnectToAddress(address);
        self.stream = stream;
        
        // Set socket timeout
        const timeout = std.posix.timeval{
            .tv_sec = @intCast(@divFloor(timeout_ms, 1000)),
            .tv_usec = @intCast((timeout_ms % 1000) * 1000),
        };
        try std.posix.setsockopt(stream.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&timeout));
        try std.posix.setsockopt(stream.handle, std.posix.SOL.SOCKET, std.posix.SO.SNDTIMEO, std.mem.asBytes(&timeout));
        
        // Wait for INFO message from server
        try self.readServerInfo();
        
        // Send CONNECT message
        const connect_cmd = try Protocol.buildConnect(self.allocator, self.connect_info);
        defer self.allocator.free(connect_cmd);
        _ = try stream.write(connect_cmd);
        
        // Wait for +OK or -ERR response
        try self.readConnectResponse();
        
        self.state = .CONNECTED;
    }

    /// Disconnect from NATS server
    pub fn disconnect(self: *Client) void {
        if (self.stream) |*stream| {
            stream.close();
            self.stream = null;
        }
        self.state = .DISCONNECTED;
    }

    /// Publish a message
    pub fn publish(self: *Client, subject: []const u8, reply: ?[]const u8, payload: []const u8) !void {
        if (self.state != .CONNECTED) {
            return errors.Error.ConnectionLost;
        }
        
        const stream = self.stream.?;
        const pub_cmd = try Protocol.buildPub(self.allocator, subject, reply, payload);
        defer self.allocator.free(pub_cmd);
        
        _ = try stream.write(pub_cmd);
    }

    /// Subscribe to a subject
    pub fn subscribe(self: *Client, subject: []const u8, queue: ?[]const u8) !*Subscription {
        if (self.state != .CONNECTED) {
            return errors.Error.ConnectionLost;
        }
        
        const sid = self.next_sid;
        self.next_sid += 1;
        
        const stream = self.stream.?;
        const sub_cmd = try Protocol.buildSub(self.allocator, subject, queue, sid);
        defer self.allocator.free(sub_cmd);
        
        _ = try stream.write(sub_cmd);
        
        const subscription = try self.allocator.create(Subscription);
        subscription.* = try Subscription.init(self.allocator, sid, subject, queue, null);
        
        try self.subscriptions.put(sid, subscription);
        return subscription;
    }

    /// Unsubscribe from a subscription
    pub fn unsubscribe(self: *Client, subscription: *Subscription, max_msgs: ?u64) !void {
        if (self.state != .CONNECTED) {
            return errors.Error.ConnectionLost;
        }
        
        const stream = self.stream.?;
        const unsub_cmd = try Protocol.buildUnsub(self.allocator, subscription.sid, max_msgs);
        defer self.allocator.free(unsub_cmd);
        
        _ = try stream.write(unsub_cmd);
        
        subscription.unsubscribe();
        _ = self.subscriptions.remove(subscription.sid);
    }

    /// Send PING
    pub fn ping(self: *Client) !void {
        if (self.state != .CONNECTED) {
            return errors.Error.ConnectionLost;
        }
        
        const stream = self.stream.?;
        const ping_cmd = try Protocol.buildPing(self.allocator);
        defer self.allocator.free(ping_cmd);
        
        _ = try stream.write(ping_cmd);
    }

    /// Process incoming messages (blocking)
    pub fn processMessages(self: *Client, timeout_ms: u64) !void {
        if (self.state != .CONNECTED) {
            return errors.Error.ConnectionLost;
        }
        
        const start_time = std.time.milliTimestamp();
        
        while (self.state == .CONNECTED) {
            // Check timeout
            if (timeout_ms > 0) {
                const elapsed = std.time.milliTimestamp() - start_time;
                if (elapsed >= timeout_ms) {
                    return errors.Error.Timeout;
                }
            }
            
            try self.readAndProcessMessage();
        }
    }

    /// Get JetStream context
    pub fn jetstream(self: *Client) !*JetStream {
        if (self.jetstream_ctx == null) {
            const js = try self.allocator.create(JetStream);
            js.* = try JetStream.init(self.allocator, self);
            self.jetstream_ctx = js;
        }
        return self.jetstream_ctx.?;
    }

    // Private methods

    fn readServerInfo(self: *Client) !void {
        const line = try self.readLine();
        
        if (!std.mem.startsWith(u8, line, "INFO ")) {
            return errors.Error.InvalidProtocol;
        }
        
        _ = line[5..]; // TODO: Parse JSON properly
        
        // Simple JSON parsing for server info - in a real implementation,
        // you'd want to use a proper JSON parser
        // For now, just extract the basic fields we need
        
        // This is a simplified approach - in practice you'd use std.json or similar
        const server_info = try self.allocator.create(ServerInfo);
        server_info.* = ServerInfo{
            .server_id = try self.allocator.dupe(u8, "nats-server"),
            .server_name = try self.allocator.dupe(u8, "nats-server"),
            .version = try self.allocator.dupe(u8, "2.10.0"),
            .proto = 1,
            .host = try self.allocator.dupe(u8, "localhost"),
            .port = 4222,
            .max_payload = 1048576,
            .client_id = 1,
            .client_ip = null,
            .connect_urls = null,
            .tls_required = false,
            .tls_available = false,
            .allocator = self.allocator,
        };
        
        self.server_info = server_info;
    }

    fn readConnectResponse(self: *Client) !void {
        const line = try self.readLine();
        
        if (std.mem.eql(u8, line, "+OK")) {
            return; // Connection successful
        } else if (std.mem.startsWith(u8, line, "-ERR ")) {
            return errors.Error.ServerError;
        } else {
            return errors.Error.InvalidProtocol;
        }
    }

    fn readAndProcessMessage(self: *Client) !void {
        const line = try self.readLine();
        
        var parts = std.mem.splitSequence(u8, line, " ");
        const op_str = parts.next() orelse return errors.Error.InvalidProtocol;
        const op = Operation.fromString(op_str) orelse return errors.Error.InvalidProtocol;
        
        switch (op) {
            .MSG => try self.handleMsgCommand(line),
            .PING => try self.handlePing(),
            .PONG => {}, // Handle pong if needed
            .OK => {}, // Handle OK response
            .ERR => try self.handleError(line),
            .INFO => {}, // Handle INFO updates
            else => return errors.Error.InvalidProtocol,
        }
    }

    fn handleMsgCommand(self: *Client, line: []const u8) !void {
        const msg_proto = try MsgProtocol.parse(line);
        
        // Read the message payload
        const payload = try self.readBytes(msg_proto.payload_len);
        
        // Create message object
        var message = try Message.init(self.allocator, msg_proto.subject, msg_proto.reply, payload);
        defer message.deinit();
        
        // Find subscription and deliver message
        if (self.subscriptions.get(msg_proto.sid)) |subscription| {
            subscription.handleMessage(&message);
        }
    }

    fn handlePing(self: *Client) !void {
        const stream = self.stream.?;
        const pong_cmd = try Protocol.buildPong(self.allocator);
        defer self.allocator.free(pong_cmd);
        _ = try stream.write(pong_cmd);
    }

    fn handleError(self: *Client, line: []const u8) !void {
        _ = self;
        _ = line; // TODO: Parse error message and handle appropriately
        return errors.Error.ServerError;
    }

    fn readLine(self: *Client) ![]const u8 {
        const stream = self.stream.?;
        var line = std.ArrayList(u8).init(self.allocator);
        defer line.deinit();
        
        var byte: u8 = undefined;
        while (true) {
            const bytes_read = try stream.read(@as([]u8, @ptrCast(&byte))[0..1]);
            if (bytes_read == 0) {
                return errors.Error.ConnectionLost;
            }
            
            if (byte == '\r') {
                // Expect \n next
                const bytes_read2 = try stream.read(@as([]u8, @ptrCast(&byte))[0..1]);
                if (bytes_read2 == 0 or byte != '\n') {
                    return errors.Error.InvalidProtocol;
                }
                break;
            } else {
                try line.append(byte);
            }
        }
        
        return line.toOwnedSlice();
    }

    fn readBytes(self: *Client, num_bytes: u64) ![]const u8 {
        const stream = self.stream.?;
        const buffer = try self.allocator.alloc(u8, num_bytes);
        
        var bytes_read: u64 = 0;
        while (bytes_read < num_bytes) {
            const n = try stream.read(buffer[bytes_read..]);
            if (n == 0) {
                self.allocator.free(buffer);
                return errors.Error.ConnectionLost;
            }
            bytes_read += n;
        }
        
        // Consume trailing CRLF
        var crlf_buf: [2]u8 = undefined;
        _ = try stream.read(crlf_buf[0..]);
        
        return buffer;
    }
};