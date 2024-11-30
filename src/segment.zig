const std = @import("std");
const msgpack = @import("msgpack");

pub const SegmentInfo = struct {
    version: u64 = 0,
    merges: u64 = 0,

    pub fn contains(self: SegmentInfo, other: SegmentInfo) bool {
        const start = self.version;
        const end = self.version + self.merges;

        const other_start = other.version;
        const other_end = other.version + other.merges;

        return other_start >= start and other_end <= end;
    }

    pub fn merge(self: SegmentInfo, other: SegmentInfo) SegmentInfo {
        std.debug.assert(self.version + self.merges + 1 == other.version);
        return .{
            .version = @min(self.version, other.version),
            .merges = self.merges + other.merges + 1,
        };
    }

    pub fn getLastCommitId(self: SegmentInfo) u64 {
        return self.version + self.merges;
    }

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_array = .{} };
    }
};

test "SegmentInfo.contains" {
    const a = SegmentInfo{ .version = 1, .merges = 0 };
    const b = SegmentInfo{ .version = 2, .merges = 0 };
    const c = SegmentInfo{ .version = 1, .merges = 1 };

    try std.testing.expect(a.contains(a));
    try std.testing.expect(!a.contains(b));
    try std.testing.expect(!a.contains(c));

    try std.testing.expect(!b.contains(a));
    try std.testing.expect(b.contains(b));
    try std.testing.expect(!b.contains(c));

    try std.testing.expect(c.contains(a));
    try std.testing.expect(c.contains(b));
    try std.testing.expect(c.contains(c));
}
