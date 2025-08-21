const std = @import("std");

/// NATS client errors
pub const NatsError = error{
    /// Connection failed
    ConnectionFailed,
    /// Connection lost
    ConnectionLost,
    /// Invalid protocol response
    InvalidProtocol,
    /// Server error
    ServerError,
    /// Permission denied
    PermissionDenied,
    /// Timeout occurred
    Timeout,
    /// Subject not found
    SubjectNotFound,
    /// Stream not found
    StreamNotFound,
    /// Consumer not found
    ConsumerNotFound,
    /// Bad request
    BadRequest,
    /// Subscription not found
    SubscriptionNotFound,
};

/// JetStream specific errors
pub const JetStreamError = error{
    /// Stream already exists
    StreamExists,
    /// Consumer already exists
    ConsumerExists,
    /// Maximum consumers reached
    MaximumConsumersReached,
    /// Message not found
    MessageNotFound,
    /// Wrong last sequence
    WrongLastSequence,
    /// Storage failure
    StorageFailure,
};

pub const Error = NatsError || JetStreamError || std.mem.Allocator.Error || std.posix.SocketError || std.fmt.ParseIntError;