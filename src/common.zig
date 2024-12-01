const std = @import("std");
const testing = std.testing;

const msgpack = @import("msgpack");
const SegmentInfo = @import("segment.zig").SegmentInfo;

pub const KeepOrDelete = enum {
    keep,
    delete,
};

pub const SearchResult = struct {
    id: u32,
    score: u32,
    version: u64,

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

    pub fn incr(self: *SearchResults, id: u32, version: u64) !void {
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
