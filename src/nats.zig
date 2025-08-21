// NATS Client Library for Zig
//
// A bare-bones implementation of NATS client with JetStream support
// for use in the acoustid-index project.
//
// This library provides:
// - Basic NATS pub/sub functionality
// - JetStream stream and consumer management
// - Message handling with acknowledgments
// - Connection management
//
// Usage:
//   const nats = @import("nats.zig");
//   const client = try nats.Client.init(allocator);
//   defer client.deinit();
//   
//   try client.connect(address, 5000); // 5 second timeout
//   try client.publish("test.subject", null, "Hello, NATS!");
//   
//   const js = try client.jetstream();
//   // ... JetStream operations

pub const Client = @import("nats/client.zig").Client;
pub const JetStream = @import("nats/jetstream.zig").JetStream;
pub const Message = @import("nats/message.zig").Message;
pub const MessageMetadata = @import("nats/message.zig").MessageMetadata;

// Subscription types
pub const Subscription = @import("nats/subscription.zig").Subscription;
pub const PullSubscription = @import("nats/subscription.zig").PullSubscription;
pub const PushSubscription = @import("nats/subscription.zig").PushSubscription;
pub const MessageCallback = @import("nats/subscription.zig").MessageCallback;

// JetStream configuration types
pub const StreamConfig = @import("nats/jetstream.zig").StreamConfig;
pub const ConsumerConfig = @import("nats/jetstream.zig").ConsumerConfig;
pub const PubAck = @import("nats/jetstream.zig").PubAck;
pub const RetentionPolicy = @import("nats/jetstream.zig").RetentionPolicy;
pub const StorageType = @import("nats/jetstream.zig").StorageType;
pub const DeliverPolicy = @import("nats/jetstream.zig").DeliverPolicy;
pub const AckPolicy = @import("nats/jetstream.zig").AckPolicy;

// Protocol types
pub const ConnectInfo = @import("nats/protocol.zig").ConnectInfo;
pub const ServerInfo = @import("nats/protocol.zig").ServerInfo;

// Error types
pub const Error = @import("nats/errors.zig").Error;
pub const NatsError = @import("nats/errors.zig").NatsError;
pub const JetStreamError = @import("nats/errors.zig").JetStreamError;