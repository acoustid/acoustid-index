const std = @import("std");
const testing = std.testing;

const msgpack = @import("msgpack");

pub const KeepOrDelete = enum {
    keep,
    delete,
};

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

test "Item binary" {
    try testing.expectEqual(8, @sizeOf(Item));
    try testing.expectEqual(64, @bitSizeOf(Item));
    try testing.expectEqual(0, @bitOffsetOf(Item, "id"));
    try testing.expectEqual(32, @bitOffsetOf(Item, "hash"));

    const item1 = Item{ .hash = 1, .id = 2 };
    const item2 = Item{ .hash = 2, .id = 1 };

    const x1: u64 = @bitCast(item1);
    const x2: u64 = @bitCast(item2);

    try testing.expectEqual(0x0000000100000002, x1);
    try testing.expectEqual(0x0000000200000001, x2);
}

test "Item array sort" {
    var items = try testing.allocator.alloc(Item, 3);
    defer testing.allocator.free(items);

    items[0] = Item{ .hash = 2, .id = 200 };
    items[1] = Item{ .hash = 2, .id = 100 };
    items[2] = Item{ .hash = 1, .id = 300 };

    std.sort.insertion(Item, items, {}, Item.cmp);

    try testing.expectEqualSlices(Item, &[_]Item{
        Item{ .hash = 1, .id = 300 },
        Item{ .hash = 2, .id = 100 },
        Item{ .hash = 2, .id = 200 },
    }, items);
}

pub const SearchResult = struct {
    id: u32,
    score: u32,
    version: u32,

    pub fn cmp(_: void, a: SearchResult, b: SearchResult) bool {
        return a.score > b.score or (a.score == b.score and a.id > b.id);
    }
};

pub const SearchResultHashMap = std.AutoArrayHashMap(u32, SearchResult);

pub const SearchResults = struct {
    results: SearchResultHashMap,

    pub fn init(allocator: std.mem.Allocator) SearchResults {
        return SearchResults{
            .results = SearchResultHashMap.init(allocator),
        };
    }

    pub fn deinit(self: *SearchResults) void {
        self.results.deinit();
    }

    pub fn incr(self: *SearchResults, id: u32, version: u32) !void {
        const r = try self.results.getOrPut(id);
        if (!r.found_existing or r.value_ptr.version < version) {
            r.value_ptr.id = id;
            r.value_ptr.score = 1;
            r.value_ptr.version = version;
        } else if (r.value_ptr.version == version) {
            r.value_ptr.score += 1;
        }
    }

    pub fn count(self: SearchResults) usize {
        return self.results.count();
    }

    pub fn get(self: SearchResults, id: u32) ?SearchResult {
        return self.results.get(id);
    }

    pub fn sort(self: *SearchResults) void {
        const Ctx = struct {
            values: []SearchResult,
            pub fn lessThan(ctx: @This(), a: usize, b: usize) bool {
                return SearchResult.cmp({}, ctx.values[a], ctx.values[b]);
            }
        };
        self.results.sort(Ctx{ .values = self.results.values() });
    }

    pub fn values(self: SearchResults) []SearchResult {
        return self.results.values();
    }

    pub fn removeOutdatedResults(self: *SearchResults, collection: anytype) void {
        var iter = self.results.iterator();
        while (iter.next()) |*entry| {
            var result = entry.value_ptr;
            if (collection.hasNewerVersion(result.id, result.version)) {
                result.score = 0;
            }
        }
    }
};

test "sort search results" {
    var results = SearchResults.init(testing.allocator);
    defer results.deinit();

    try results.incr(1, 1);
    try results.incr(2, 1);
    try results.incr(2, 1);

    results.sort();

    try testing.expectEqualSlices(SearchResult, &[_]SearchResult{
        SearchResult{ .id = 2, .score = 2, .version = 1 },
        SearchResult{ .id = 1, .score = 1, .version = 1 },
    }, results.values());
}

pub const SegmentId = packed struct(u64) {
    version: u32,
    included_merges: u32 = 0,

    pub fn cmp(_: void, a: SegmentId, b: SegmentId) bool {
        const xa: u64 = @bitCast(a);
        const xb: u64 = @bitCast(b);
        return xa < xb;
    }

    pub fn eq(a: SegmentId, b: SegmentId) bool {
        const xa: u64 = @bitCast(a);
        const xb: u64 = @bitCast(b);
        return xa == xb;
    }

    pub fn contains(self: SegmentId, other: SegmentId) bool {
        const start = self.version;
        const end = self.version + self.included_merges;

        const other_start = other.version;
        const other_end = other.version + other.included_merges;

        return other_start >= start and other_end <= end;
    }

    pub fn first() SegmentId {
        return .{
            .version = 1,
            .included_merges = 0,
        };
    }

    pub fn next(a: SegmentId) SegmentId {
        return .{
            .version = a.version + a.included_merges + 1,
            .included_merges = 0,
        };
    }

    pub fn merge(a: SegmentId, b: SegmentId) SegmentId {
        return .{
            .version = @min(a.version, b.version),
            .included_merges = 1 + a.included_merges + b.included_merges,
        };
    }

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_array = .{} };
    }
};

test "SegmentId.contains" {
    const a = SegmentId{ .version = 1, .included_merges = 0 };
    const b = SegmentId{ .version = 2, .included_merges = 0 };
    const c = SegmentId{ .version = 1, .included_merges = 1 };

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
