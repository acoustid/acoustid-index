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

    pub fn cmpByHash(_: void, a: Item, b: Item) bool {
        return a.hash < b.hash;
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

    pub fn cmp(_: void, a: SearchResult, b: SearchResult) bool {
        return a.score > b.score or (a.score == b.score and a.docId > b.docId);
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

    pub fn incr(self: *SearchResults, doc_id: u32, version: u32) !void {
        const r = try self.results.getOrPut(doc_id);
        if (!r.found_existing or r.value_ptr.version < version) {
            r.value_ptr.docId = doc_id;
            r.value_ptr.score = 1;
            r.value_ptr.version = version;
        } else if (r.value_ptr.version == version) {
            r.value_ptr.score += 1;
        }
    }

    pub fn count(self: *SearchResults) usize {
        return self.results.count();
    }

    pub fn get(self: *SearchResults, doc_id: u32) ?SearchResult {
        return self.results.get(doc_id);
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

    pub fn values(self: *SearchResults) []SearchResult {
        return self.results.values();
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
        SearchResult{ .docId = 2, .score = 2, .version = 1 },
        SearchResult{ .docId = 1, .score = 1, .version = 1 },
    }, results.values());
}

pub const Insert = struct {
    id: u32,
    hashes: []const u32,
};

pub const Delete = struct {
    id: u32,
};

pub const Change = union(enum) {
    insert: Insert,
    delete: Delete,
};
