const std = @import("std");
const testing = std.testing;

const msgpack = @import("msgpack");
const SegmentInfo = @import("segment.zig").SegmentInfo;

pub const DocInfo = struct {
    version: u64,
    deleted: bool,
};

pub const KeepOrDelete = enum {
    keep,
    delete,
};

pub const SearchResult = struct {
    id: u32,
    score: u32,
};

pub const SearchOptions = struct {
    max_results: u32 = 10,
    min_score: u32 = 1,
    min_score_pct: u32 = 10,
};

pub const SearchResults = struct {
    allocator: std.mem.Allocator,
    options: SearchOptions,
    results: std.ArrayListUnmanaged(SearchResult) = .{},
    hits: std.AutoHashMapUnmanaged(u32, Hit) = .{},

    const Hit = packed struct {
        version: u64,
        score: u32,
    };

    pub fn init(allocator: std.mem.Allocator, options: SearchOptions) SearchResults {
        return SearchResults{
            .allocator = allocator,
            .options = options,
        };
    }

    pub fn deinit(self: *SearchResults) void {
        self.hits.deinit(self.allocator);
        self.results.deinit(self.allocator);
    }

    pub fn incr(self: *SearchResults, id: u32, version: u64) !void {
        const r = try self.hits.getOrPut(self.allocator, id);
        if (!r.found_existing or r.value_ptr.version < version) {
            r.value_ptr.score = 1;
            r.value_ptr.version = version;
        } else if (r.value_ptr.version == version) {
            r.value_ptr.score += 1;
        }
    }

    pub fn get(self: SearchResults, id: u32) ?SearchResult {
        const hit = self.hits.get(id) orelse return null;
        return .{
            .id = id,
            .score = hit.score,
            .version = hit.version,
        };
    }

    pub fn finish(self: *SearchResults, collection: anytype) !void {
        var ids = try std.ArrayListUnmanaged(u32).initCapacity(self.allocator, self.hits.count());
        defer ids.deinit(self.allocator);

        var min_score = self.options.min_score;

        var iter = self.hits.iterator();
        while (iter.next()) |entry| {
            if (entry.value_ptr.*.score >= min_score) {
                ids.appendAssumeCapacity(entry.key_ptr.*);
            }
        }

        std.sort.pdq(u32, ids.items, self, compareResults);

        self.results.clearRetainingCapacity();
        try self.results.ensureTotalCapacity(self.allocator, self.options.max_results);

        for (ids.items) |id| {
            if (self.results.items.len == self.options.max_results) {
                break;
            }
            const hit = self.hits.get(id) orelse unreachable;
            if (collection.hasNewerVersion(id, hit.version)) {
                continue;
            }
            if (hit.score < min_score) {
                break;
            }
            if (self.results.items.len == 0) {
                min_score = @max(min_score, hit.score * self.options.min_score_pct / 100);
            }
            self.results.appendAssumeCapacity(.{
                .id = id,
                .score = hit.score,
            });
        }
    }

    pub fn compareResults(self: *SearchResults, a: u32, b: u32) bool {
        const a_hit = self.hits.get(a) orelse unreachable;
        const b_hit = self.hits.get(b) orelse unreachable;
        return a_hit.score > b_hit.score or (a_hit.score == b_hit.score and a < b);
    }

    pub fn getResults(self: *SearchResults) []SearchResult {
        return self.results.items;
    }
};
