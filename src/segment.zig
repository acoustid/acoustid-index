const std = @import("std");
const msgpack = @import("msgpack");

// Identity of a segment, and what it covers.
//
// `commit_id`/`merges` are the INTERNAL commit-id interval [commit_id, commit_id+merges].
// Commit ids are minted locally, one per index write, and are dense: segments tile
// the sequence without gaps, which is what `merge` asserts on below and what
// `contains` reasons about.
//
// `version` is the EXTERNAL position in the upstream changelog that this
// segment's contents are complete up to. It is deliberately NOT the commit id: one
// commit can cover many log positions (a consumer coalesces a batch and applies it
// as one write) and many commits can share one log position (a bootstrap loads a
// whole table snapshot taken at a single position). Conflating them forced the
// external, arbitrarily-spaced position into a field that must be dense.
pub const SegmentInfo = struct {
    commit_id: u64 = 0,
    merges: u64 = 0,
    version: u64 = 0,

    pub fn contains(self: SegmentInfo, other: SegmentInfo) bool {
        const start = self.commit_id;
        const end = self.commit_id + self.merges;

        const other_start = other.commit_id;
        const other_end = other.commit_id + other.merges;

        return other_start >= start and other_end <= end;
    }

    pub fn merge(self: SegmentInfo, other: SegmentInfo) SegmentInfo {
        std.debug.assert(self.commit_id + self.merges + 1 == other.commit_id);
        return .{
            .commit_id = @min(self.commit_id, other.commit_id),
            .merges = self.merges + other.merges + 1,
            // `other` is the internally-adjacent later segment, so its position is
            // the newer one; @max states that without relying on the ordering.
            .version = @max(self.version, other.version),
        };
    }

    pub fn getLastCommitId(self: SegmentInfo) u64 {
        return self.commit_id + self.merges;
    }

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_array = .{} };
    }
};

test "SegmentInfo.contains" {
    const a = SegmentInfo{ .commit_id = 1, .merges = 0 };
    const b = SegmentInfo{ .commit_id = 2, .merges = 0 };
    const c = SegmentInfo{ .commit_id = 1, .merges = 1 };

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

    pub fn lessThan(_: void, a: Item, b: Item) bool {
        const xa: u64 = @bitCast(a);
        const xb: u64 = @bitCast(b);
        return xa < xb;
    }

    pub fn order(a: Item, b: Item) std.math.Order {
        const xa: u64 = @bitCast(a);
        const xb: u64 = @bitCast(b);
        return std.math.order(xa, xb);
    }

    pub fn orderByHash(a: Item, b: Item) std.math.Order {
        return std.math.order(a.hash, b.hash);
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

    std.sort.insertion(Item, items, {}, Item.lessThan);

    try std.testing.expectEqualSlices(Item, &[_]Item{
        Item{ .hash = 1, .id = 300 },
        Item{ .hash = 2, .id = 100 },
        Item{ .hash = 2, .id = 200 },
    }, items);
}
