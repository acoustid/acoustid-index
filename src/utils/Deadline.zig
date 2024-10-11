const std = @import("std");
const time = std.time;

const Self = @This();

deadline: i64 = 0,

pub fn setTimeoutMs(self: *Self, timeout: i64) void {
    self.deadline = if (timeout > 0) time.milliTimestamp() + timeout else 0;
}

pub fn isExpired(self: *const Self) bool {
    return self.deadline > 0 and time.milliTimestamp() >= self.deadline;
}
