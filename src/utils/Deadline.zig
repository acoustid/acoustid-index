const std = @import("std");
const time = std.time;

const Self = @This();

deadline_ms: i64 = 0,

pub fn init(timeout_ms: i64) Self {
    return Self{ .deadline_ms = if (timeout_ms > 0) time.milliTimestamp() + timeout_ms else 0 };
}

pub fn setTimeout(self: *Self, timeout_ms: i64) void {
    self.deadline_ms = if (timeout_ms > 0) time.milliTimestamp() + timeout_ms else 0;
}

pub fn isExpired(self: *const Self) bool {
    return self.deadline_ms > 0 and time.milliTimestamp() >= self.deadline_ms;
}
