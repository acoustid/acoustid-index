const std = @import("std");
const testing = std.testing;
const net = std.net;
const nats = @import("nats.zig");

test "nats client creation and cleanup" {
    const allocator = testing.allocator;
    const client = try nats.Client.init(allocator);
    defer client.deinit();
    
    try testing.expect(client.state == .DISCONNECTED);
    try testing.expect(client.next_sid == 1);
}

test "nats protocol message building" {
    const allocator = testing.allocator;
    const protocol = @import("nats/protocol.zig").Protocol;
    
    // Test CONNECT message
    const connect_info = nats.ConnectInfo{
        .verbose = false,
        .lang = "zig",
        .version = "0.1.0",
    };
    
    const connect_msg = try protocol.buildConnect(allocator, connect_info);
    defer allocator.free(connect_msg);
    
    try testing.expect(std.mem.startsWith(u8, connect_msg, "CONNECT"));
    try testing.expect(std.mem.endsWith(u8, connect_msg, "\r\n"));
    
    // Test PUB message
    const pub_msg = try protocol.buildPub(allocator, "test.subject", null, "Hello, World!");
    defer allocator.free(pub_msg);
    
    const expected_pub = "PUB test.subject 13\r\nHello, World!\r\n";
    try testing.expectEqualStrings(expected_pub, pub_msg);
    
    // Test PUB message with reply
    const pub_reply_msg = try protocol.buildPub(allocator, "test.subject", "reply.subject", "Hello!");
    defer allocator.free(pub_reply_msg);
    
    const expected_pub_reply = "PUB test.subject reply.subject 6\r\nHello!\r\n";
    try testing.expectEqualStrings(expected_pub_reply, pub_reply_msg);
    
    // Test SUB message
    const sub_msg = try protocol.buildSub(allocator, "test.subject", null, 1);
    defer allocator.free(sub_msg);
    
    const expected_sub = "SUB test.subject 1\r\n";
    try testing.expectEqualStrings(expected_sub, sub_msg);
    
    // Test UNSUB message
    const unsub_msg = try protocol.buildUnsub(allocator, 1, null);
    defer allocator.free(unsub_msg);
    
    const expected_unsub = "UNSUB 1\r\n";
    try testing.expectEqualStrings(expected_unsub, unsub_msg);
}

test "nats message parsing" {
    const MsgProtocol = @import("nats/protocol.zig").MsgProtocol;
    
    // Test simple MSG parsing
    const simple_msg = "MSG test.subject 1 13";
    const parsed_simple = try MsgProtocol.parse(simple_msg);
    
    try testing.expectEqualStrings("test.subject", parsed_simple.subject);
    try testing.expect(parsed_simple.sid == 1);
    try testing.expect(parsed_simple.reply == null);
    try testing.expect(parsed_simple.payload_len == 13);
    
    // Test MSG with reply parsing
    const reply_msg = "MSG test.subject 1 reply.subject 13";
    const parsed_reply = try MsgProtocol.parse(reply_msg);
    
    try testing.expectEqualStrings("test.subject", parsed_reply.subject);
    try testing.expect(parsed_reply.sid == 1);
    try testing.expectEqualStrings("reply.subject", parsed_reply.reply.?);
    try testing.expect(parsed_reply.payload_len == 13);
}

test "nats subscription management" {
    const allocator = testing.allocator;
    
    var subscription = try nats.Subscription.init(allocator, 1, "test.subject", null, null);
    defer subscription.deinit();
    
    try testing.expect(subscription.sid == 1);
    try testing.expectEqualStrings("test.subject", subscription.subject);
    try testing.expect(subscription.queue == null);
    try testing.expect(subscription.active == true);
    try testing.expect(subscription.msgs_received == 0);
    
    // Test unsubscribe
    subscription.unsubscribe();
    try testing.expect(subscription.active == false);
}

test "jetstream stream config json" {
    const allocator = testing.allocator;
    
    var subjects = [_][]const u8{ "test.stream.>", "another.subject" };
    const config = nats.StreamConfig{
        .name = "test-stream",
        .subjects = subjects[0..],
        .retention = .LIMITS,
        .storage = .FILE,
        .max_msgs = 1000,
        .max_age = 86400 * 1000000000, // 1 day in nanoseconds
    };
    
    const json = try config.toJson(allocator);
    defer allocator.free(json);
    
    try testing.expect(std.mem.indexOf(u8, json, "\"name\":\"test-stream\"") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"retention\":\"limits\"") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"storage\":\"file\"") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"max_msgs\":1000") != null);
}

test "jetstream consumer config json" {
    const allocator = testing.allocator;
    
    const config = nats.ConsumerConfig{
        .durable_name = "test-consumer",
        .deliver_policy = .ALL,
        .ack_policy = .EXPLICIT,
        .max_deliver = 3,
        .max_waiting = 512,
        .max_ack_pending = 1000,
    };
    
    const json = try config.toJson(allocator);
    defer allocator.free(json);
    
    try testing.expect(std.mem.indexOf(u8, json, "\"durable_name\":\"test-consumer\"") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"deliver_policy\":\"all\"") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"ack_policy\":\"explicit\"") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"max_deliver\":3") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"max_waiting\":512") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"max_ack_pending\":1000") != null);
}