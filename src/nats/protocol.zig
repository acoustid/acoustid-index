const std = @import("std");
const errors = @import("errors.zig");

pub const CRLF = "\r\n";

/// NATS protocol operations
pub const Operation = enum {
    CONNECT,
    PUB,
    SUB,
    UNSUB,
    MSG,
    PING,
    PONG,
    INFO,
    OK,
    ERR,
    
    pub fn fromString(str: []const u8) ?Operation {
        if (std.mem.eql(u8, str, "CONNECT")) return .CONNECT;
        if (std.mem.eql(u8, str, "PUB")) return .PUB;
        if (std.mem.eql(u8, str, "SUB")) return .SUB;
        if (std.mem.eql(u8, str, "UNSUB")) return .UNSUB;
        if (std.mem.eql(u8, str, "MSG")) return .MSG;
        if (std.mem.eql(u8, str, "PING")) return .PING;
        if (std.mem.eql(u8, str, "PONG")) return .PONG;
        if (std.mem.eql(u8, str, "INFO")) return .INFO;
        if (std.mem.eql(u8, str, "+OK")) return .OK;
        if (std.mem.eql(u8, str, "-ERR")) return .ERR;
        return null;
    }
};

/// Connection information
pub const ConnectInfo = struct {
    verbose: bool = false,
    pedantic: bool = false,
    tls_required: bool = false,
    name: ?[]const u8 = null,
    lang: []const u8 = "zig",
    version: []const u8 = "0.1.0",
    protocol: u8 = 1,
    echo: bool = true,
    sig: ?[]const u8 = null,
    jwt: ?[]const u8 = null,
    no_responders: bool = true,
    headers: bool = true,
    nkey: ?[]const u8 = null,
    
    pub fn toJson(self: ConnectInfo, allocator: std.mem.Allocator) ![]u8 {
        var list = std.ArrayList(u8).init(allocator);
        defer list.deinit();
        
        try std.json.stringify(self, .{}, list.writer());
        return list.toOwnedSlice();
    }
};

/// Server information received in INFO message
pub const ServerInfo = struct {
    server_id: []const u8,
    server_name: []const u8,
    version: []const u8,
    proto: u8,
    host: []const u8,
    port: u16,
    max_payload: u32,
    client_id: u64,
    client_ip: ?[]const u8,
    connect_urls: ?[][]const u8,
    tls_required: bool,
    tls_available: bool,
    
    allocator: std.mem.Allocator,
    
    pub fn deinit(self: *ServerInfo) void {
        self.allocator.free(self.server_id);
        self.allocator.free(self.server_name);
        self.allocator.free(self.version);
        self.allocator.free(self.host);
        if (self.client_ip) |ip| {
            self.allocator.free(ip);
        }
        if (self.connect_urls) |urls| {
            for (urls) |url| {
                self.allocator.free(url);
            }
            self.allocator.free(urls);
        }
    }
};

/// Protocol message parsers and builders
pub const Protocol = struct {
    pub fn buildConnect(allocator: std.mem.Allocator, connect_info: ConnectInfo) ![]u8 {
        const json = try connect_info.toJson(allocator);
        defer allocator.free(json);
        return try std.fmt.allocPrint(allocator, "CONNECT {s}{s}", .{ json, CRLF });
    }
    
    pub fn buildPub(allocator: std.mem.Allocator, subject: []const u8, reply: ?[]const u8, payload: []const u8) ![]u8 {
        if (reply) |r| {
            return try std.fmt.allocPrint(allocator, "PUB {s} {s} {d}{s}{s}{s}", .{ subject, r, payload.len, CRLF, payload, CRLF });
        } else {
            return try std.fmt.allocPrint(allocator, "PUB {s} {d}{s}{s}{s}", .{ subject, payload.len, CRLF, payload, CRLF });
        }
    }
    
    pub fn buildSub(allocator: std.mem.Allocator, subject: []const u8, queue: ?[]const u8, sid: u64) ![]u8 {
        if (queue) |q| {
            return try std.fmt.allocPrint(allocator, "SUB {s} {s} {d}{s}", .{ subject, q, sid, CRLF });
        } else {
            return try std.fmt.allocPrint(allocator, "SUB {s} {d}{s}", .{ subject, sid, CRLF });
        }
    }
    
    pub fn buildUnsub(allocator: std.mem.Allocator, sid: u64, max_msgs: ?u64) ![]u8 {
        if (max_msgs) |max| {
            return try std.fmt.allocPrint(allocator, "UNSUB {d} {d}{s}", .{ sid, max, CRLF });
        } else {
            return try std.fmt.allocPrint(allocator, "UNSUB {d}{s}", .{ sid, CRLF });
        }
    }
    
    pub fn buildPing(allocator: std.mem.Allocator) ![]u8 {
        return try std.fmt.allocPrint(allocator, "PING{s}", .{CRLF});
    }
    
    pub fn buildPong(allocator: std.mem.Allocator) ![]u8 {
        return try std.fmt.allocPrint(allocator, "PONG{s}", .{CRLF});
    }
};

/// Message structure for parsing incoming MSG protocol messages
pub const MsgProtocol = struct {
    subject: []const u8,
    sid: u64,
    reply: ?[]const u8,
    payload_len: u64,
    
    pub fn parse(line: []const u8) !MsgProtocol {
        var parts = std.mem.splitSequence(u8, line, " ");
        
        // Skip "MSG"
        _ = parts.next() orelse return errors.Error.InvalidProtocol;
        
        const subject = parts.next() orelse return errors.Error.InvalidProtocol;
        const sid_str = parts.next() orelse return errors.Error.InvalidProtocol;
        const sid = try std.fmt.parseInt(u64, sid_str, 10);
        
        // Check if there's a reply subject
        const next_part = parts.next();
        if (next_part == null) return errors.Error.InvalidProtocol;
        
        const maybe_payload_len = parts.next();
        if (maybe_payload_len) |payload_len_str| {
            // Format: MSG <subject> <sid> <reply-to> <#bytes>
            const reply = next_part;
            const payload_len = try std.fmt.parseInt(u64, payload_len_str, 10);
            return MsgProtocol{
                .subject = subject,
                .sid = sid,
                .reply = reply,
                .payload_len = payload_len,
            };
        } else {
            // Format: MSG <subject> <sid> <#bytes>
            const payload_len = try std.fmt.parseInt(u64, next_part.?, 10);
            return MsgProtocol{
                .subject = subject,
                .sid = sid,
                .reply = null,
                .payload_len = payload_len,
            };
        }
    }
};