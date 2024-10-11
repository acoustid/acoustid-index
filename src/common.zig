const std = @import("std");
const testing = std.testing;

pub const Item = packed struct(u64) {
    docId: u32,
    hash: u32,

    pub fn cmp(_: void, a: Item, b: Item) bool {
        const xa: u64 = @bitCast(a);
        const xb: u64 = @bitCast(b);
        return xa < xb;
    }
};

test "Item binary" {
    try testing.expectEqual(8, @sizeOf(Item));
    try testing.expectEqual(64, @bitSizeOf(Item));
    try testing.expectEqual(0, @bitOffsetOf(Item, "docId"));
    try testing.expectEqual(32, @bitOffsetOf(Item, "hash"));

    const item1 = Item{ .hash = 1, .docId = 2 };
    const item2 = Item{ .hash = 2, .docId = 1 };

    const x1: u64 = @bitCast(item1);
    const x2: u64 = @bitCast(item2);

    try testing.expectEqual(0x0000000100000002, x1);
    try testing.expectEqual(0x0000000200000001, x2);
}

test "Item array sort" {
    var items = try testing.allocator.alloc(Item, 3);
    defer testing.allocator.free(items);

    items[0] = Item{ .hash = 2, .docId = 200 };
    items[1] = Item{ .hash = 2, .docId = 100 };
    items[2] = Item{ .hash = 1, .docId = 300 };

    std.sort.insertion(Item, items, {}, Item.cmp);

    try testing.expectEqualSlices(Item, &[_]Item{
        Item{ .hash = 1, .docId = 300 },
        Item{ .hash = 2, .docId = 100 },
        Item{ .hash = 2, .docId = 200 },
    }, items);
}

pub const SearchResult = struct {
    docId: u32,
    score: u32,
    version: u32,
};

pub const SearchResultHashMap = std.AutoArrayHashMap(u32, SearchResult);
