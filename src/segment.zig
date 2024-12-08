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

pub const Item = packed struct(u64) {
    id: u32,
    hash: u32,

    pub fn cmp(_: void, a: Item, b: Item) bool {
        const xa: u64 = @bitCast(a);
        const xb: u64 = @bitCast(b);
        return xa < xb;
    }

    pub fn cmpByHash(_: void, a: Item, b: Item) bool {
        return a.hash < b.hash;
    }
};

pub const SegmentStatus = struct {
    frozen: bool = false,
};

test "Item binary" {
    try std.testing.expectEqual(8, @sizeOf(Item));
    try std.testing.expectEqual(64, @bitSizeOf(Item));
    try std.testing.expectEqual(0, @bitOffsetOf(Item, "id"));
    try std.testing.expectEqual(32, @bitOffsetOf(Item, "hash"));

    const item1 = Item{ .hash = 1, .id = 2 };
    const item2 = Item{ .hash = 2, .id = 1 };

    const x1: u64 = @bitCast(item1);
    const x2: u64 = @bitCast(item2);

    try std.testing.expectEqual(0x0000000100000002, x1);
    try std.testing.expectEqual(0x0000000200000001, x2);
}

test "Item array sort" {
    var items = try std.testing.allocator.alloc(Item, 3);
    defer std.testing.allocator.free(items);

    items[0] = Item{ .hash = 2, .id = 200 };
    items[1] = Item{ .hash = 2, .id = 100 };
    items[2] = Item{ .hash = 1, .id = 300 };

    std.sort.insertion(Item, items, {}, Item.cmp);

    try std.testing.expectEqualSlices(Item, &[_]Item{
        Item{ .hash = 1, .id = 300 },
        Item{ .hash = 2, .id = 100 },
        Item{ .hash = 2, .id = 200 },
    }, items);
}
