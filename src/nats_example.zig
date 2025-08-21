const std = @import("std");
const nats = @import("nats.zig");
const net = std.net;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    std.log.info("Creating NATS client...", .{});
    const client = try nats.Client.init(allocator);
    defer client.deinit();
    
    // Note: This is just a demonstration of the API structure
    // Actual connection would require a running NATS server
    std.log.info("NATS client created successfully", .{});
    
    // Demonstrate JetStream API structure
    std.log.info("Creating JetStream context...", .{});
    const js = try client.jetstream();
    _ = js; // Use js to avoid unused warning
    
    // Example stream configuration
    var subjects = [_][]const u8{ "fpindex.*.op", "fpindex.discovery.>" };
    const stream_config = nats.StreamConfig{
        .name = "fpindex_demo_stream",
        .subjects = subjects[0..],
        .retention = .LIMITS,
        .storage = .FILE,
        .max_msgs = 1000000,
        .max_age = 7 * 24 * 3600 * 1000000000, // 7 days in nanoseconds
        .duplicate_window = 5 * 60 * 1000000000, // 5 minutes in nanoseconds
    };
    
    std.log.info("Stream config created: {s}", .{stream_config.name});
    
    // Example consumer configuration
    const consumer_config = nats.ConsumerConfig{
        .durable_name = "fpindex-demo-consumer",
        .deliver_policy = .ALL,
        .ack_policy = .EXPLICIT,
        .max_waiting = 512,
        .max_ack_pending = 1000,
    };
    
    std.log.info("Consumer config created: {s}", .{consumer_config.durable_name.?});
    
    // Convert configurations to JSON to show structure
    const stream_json = try stream_config.toJson(allocator);
    defer allocator.free(stream_json);
    std.log.info("Stream config JSON: {s}", .{stream_json});
    
    const consumer_json = try consumer_config.toJson(allocator);
    defer allocator.free(consumer_json);
    std.log.info("Consumer config JSON: {s}", .{consumer_json});
    
    std.log.info("NATS client library demo completed successfully", .{});
}

// Example usage that would work with a real NATS server:
//
// try client.connect(try net.Address.parseIp4("127.0.0.1", 4222), 5000);
// 
// // Create stream
// try js.addStream(stream_config);
// 
// // Publish messages
// try js.publish("fpindex.demo.op", "Hello from JetStream!", null);
//
// // Create pull subscription
// const sub = try js.pullSubscribe("fpindex.demo.op", "fpindex_demo_stream", consumer_config);
// defer sub.deinit();
//
// // Fetch messages
// const messages = try js.fetch(sub, 10, 5000); // batch=10, timeout=5s
// defer allocator.free(messages);
//
// client.disconnect();