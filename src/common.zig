const std = @import("std");
const zio = @import("zio");
const testing = std.testing;

// Recursively delete `parent/name` and everything under it, then the dir itself.
// Names are collected before deleting since deleting during iteration is unsafe.
pub fn deleteDirTree(allocator: std.mem.Allocator, parent: zio.Dir, name: []const u8) !void {
    var sub = try parent.openDir(name, .{ .iterate = true });

    var files: std.ArrayListUnmanaged([]u8) = .empty;
    var dirs: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (files.items) |n| allocator.free(n);
        for (dirs.items) |n| allocator.free(n);
        files.deinit(allocator);
        dirs.deinit(allocator);
    }

    var it = sub.iterate();
    while (try it.next()) |entry| {
        const dup = try allocator.dupe(u8, entry.name);
        if (entry.kind == .directory) {
            try dirs.append(allocator, dup);
        } else {
            try files.append(allocator, dup);
        }
    }
    for (files.items) |n| sub.deleteFile(n) catch |err| if (err != error.FileNotFound) return err;
    for (dirs.items) |n| try deleteDirTree(allocator, sub, n);

    sub.close();
    try parent.deleteDir(name);
}

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

// Docid keys are dense, well-distributed integers, so AutoHashMap's default
// (Wyhash over the key bytes) is pure overhead. std.HashMap masks the low bits of
// the hash for the bucket and takes the top 7 bits for the fingerprint, so we need
// entropy across the whole word: a splitmix64 finalizer gives that in a few
// multiplies.
pub const HitContext = struct {
    pub fn hash(_: HitContext, key: u32) u64 {
        var x: u64 = key;
        x = (x ^ (x >> 30)) *% 0xbf58476d1ce4e5b9;
        x = (x ^ (x >> 27)) *% 0x94d049bb133111eb;
        return x ^ (x >> 31);
    }
    pub fn eql(_: HitContext, a: u32, b: u32) bool {
        return a == b;
    }
};

pub const SearchResults = struct {
    allocator: std.mem.Allocator,
    options: SearchOptions,
    results: std.ArrayListUnmanaged(SearchResult) = .empty,
    hits: std.HashMapUnmanaged(u32, Hit, HitContext, std.hash_map.default_max_load_percentage) = .{},
    pool_next: ?*SearchResults = null, // intrusive free-list link, see SearchResultsPool

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

    // Reset for reuse without giving back the map/list capacity (see SearchResultsPool).
    pub fn clearRetainingCapacity(self: *SearchResults) void {
        self.hits.clearRetainingCapacity();
        self.results.clearRetainingCapacity();
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

// Reuses SearchResults across queries so the hit map keeps its capacity instead
// of being reallocated and rehashed every search. Pooled entries own their memory
// from a long-lived allocator (not a per-request arena) so it survives the
// request. acquire() locks cancelably; release() runs from a defer, so it can't.
pub const SearchResultsPool = struct {
    allocator: std.mem.Allocator,
    mutex: zio.Mutex = .init,
    free: ?*SearchResults = null,

    pub fn init(allocator: std.mem.Allocator) SearchResultsPool {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *SearchResultsPool) void {
        var node = self.free;
        while (node) |r| {
            node = r.pool_next;
            r.deinit();
            self.allocator.destroy(r);
        }
    }

    pub fn acquire(self: *SearchResultsPool, options: SearchOptions) !*SearchResults {
        {
            try self.mutex.lock();
            defer self.mutex.unlock();
            if (self.free) |r| {
                self.free = r.pool_next;
                r.pool_next = null;
                r.options = options;
                return r;
            }
        }
        const r = try self.allocator.create(SearchResults);
        r.* = SearchResults.init(self.allocator, options);
        return r;
    }

    pub fn release(self: *SearchResultsPool, r: *SearchResults) void {
        r.clearRetainingCapacity();
        self.mutex.lockUncancelable();
        defer self.mutex.unlock();
        r.pool_next = self.free;
        self.free = r;
    }
};
