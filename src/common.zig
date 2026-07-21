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
    // Doubles as finish()'s scratch: it starts as every candidate and is compacted
    // down to the top-k in place, so ranking needs no second array. Sized with the
    // hit map rather than max_results, hence capped on reuse like the map is.
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

    // Reset for reuse, keeping the map/list capacity so the next query doesn't have
    // to grow them again. A query that touched an unusually large number of docs is
    // the exception: past `max_hits_capacity` the memory is given back rather than
    // parked in the pool (see SearchResultsPool.max_retained_hits). Both grow with
    // the number of matched docs - `results` because finish() ranks in it - so both
    // are capped.
    pub fn resetForReuse(self: *SearchResults, max_hits_capacity: u32) void {
        if (self.hits.capacity() > max_hits_capacity) {
            self.hits.clearAndFree(self.allocator);
        } else {
            self.hits.clearRetainingCapacity();
        }
        if (self.results.capacity > max_hits_capacity) {
            self.results.clearAndFree(self.allocator);
        } else {
            self.results.clearRetainingCapacity();
        }
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
        // Every doc over the absolute floor becomes a candidate, carrying its score
        // so the sort is pure array work - the score used to live only in the map,
        // which cost two probes per comparison.
        self.results.clearRetainingCapacity();
        try self.results.ensureTotalCapacity(self.allocator, self.hits.count());

        var min_score = self.options.min_score;

        var iter = self.hits.iterator();
        while (iter.next()) |entry| {
            if (entry.value_ptr.score >= min_score) {
                self.results.appendAssumeCapacity(.{ .id = entry.key_ptr.*, .score = entry.value_ptr.score });
            }
        }

        std.sort.pdq(SearchResult, self.results.items, {}, compareResults);

        // Take the top-k in place: `out` only advances when a candidate survives, so
        // it never overtakes the read cursor and the survivors compact to the front.
        // The map is probed only for the candidates actually examined, for their
        // version - the rest of the sorted tail is never touched.
        var out: usize = 0;
        for (self.results.items) |candidate| {
            if (out == self.options.max_results) break;
            const hit = self.hits.get(candidate.id) orelse unreachable;
            // A hit from a superseded version of the doc: skip it, but keep scanning.
            if (collection.hasNewerVersion(candidate.id, hit.version)) continue;
            // Sorted by score descending, so nothing further can clear the bar.
            if (candidate.score < min_score) break;
            // Relative cutoff, anchored on the best score that actually survived.
            if (out == 0) min_score = @max(min_score, candidate.score * self.options.min_score_pct / 100);
            self.results.items[out] = candidate;
            out += 1;
        }
        self.results.items.len = out;
    }

    fn compareResults(_: void, a: SearchResult, b: SearchResult) bool {
        return a.score > b.score or (a.score == b.score and a.id < b.id);
    }

    pub fn getResults(self: *SearchResults) []SearchResult {
        return self.results.items;
    }
};

// Reuses SearchResults across queries so the hit map keeps its capacity instead
// of being reallocated and rehashed every search. Pooled entries own their memory
// from a long-lived allocator (not a per-request arena) so it survives the
// request. acquire() locks cancelably; release() runs from a defer, so it can't.
//
// The pool grows to whatever concurrency the traffic demands - a burst of 5000
// searches parks 5000 collectors - and shrinks back afterwards via trim(), so a
// peak doesn't cost anything once it passes. See trim() for how it decides.
pub const SearchResultsPool = struct {
    allocator: std.mem.Allocator,
    mutex: zio.Mutex = .init,
    free: ?*SearchResults = null,
    free_count: usize = 0,
    // Smallest free_count seen since the last trim (see trim()).
    low_water: usize = 0,
    sweeper: ?zio.JoinHandle(zio.Cancelable!void) = null,
    // How often the sweeper reclaims idle collectors. Sets the settling rate: an
    // idle pool halves every interval (see trim()), so a burst of N collectors is
    // gone within roughly log2(N) sweeps of the traffic dropping off.
    trim_interval: zio.Duration = .fromMilliseconds(30_000),
    // Cap on the hit-map capacity a parked collector may hold, in slots. One wide
    // query against a large index can grow the map to millions of entries; without
    // this, that peak stays resident in the pool for the life of the process.
    max_retained_hits: u32 = 64 * 1024,

    pub fn init(allocator: std.mem.Allocator) SearchResultsPool {
        return .{ .allocator = allocator };
    }

    /// Start the sweeper coroutine. Call once the pool is at its final address
    /// (the coroutine captures `self`). Optional - without it the pool never
    /// shrinks, which is fine for tests and short-lived tools.
    pub fn start(self: *SearchResultsPool) !void {
        self.sweeper = try zio.spawn(sweepLoop, .{self});
    }

    pub fn deinit(self: *SearchResultsPool) void {
        if (self.sweeper) |*task| {
            task.cancel();
            self.sweeper = null;
        }
        var node = self.free;
        while (node) |r| {
            node = r.pool_next;
            r.deinit();
            self.allocator.destroy(r);
        }
        self.free = null;
        self.free_count = 0;
        self.low_water = 0;
    }

    fn sweepLoop(self: *SearchResultsPool) zio.Cancelable!void {
        while (true) {
            try zio.sleep(self.trim_interval);
            self.trim();
        }
    }

    // Free some of the collectors nobody needed during the last interval. `low_water`
    // is the smallest the free list got, so that many entries sat untouched the whole
    // time; give back half of them (rounded up, so it still reaches zero) and let
    // repeated sweeps decay the rest. Halving rather than dropping the lot keeps a
    // single quiet interval from throwing away a pool that traffic is about to want
    // again - the cost of guessing wrong is one interval of re-allocation, but the
    // pool still empties within a few sweeps of going idle.
    //
    // The list is strict LIFO - release() pushes the head, acquire() pops it - so it
    // stays ordered by last use, and the idle ones are exactly the tail: find the cut
    // point in one walk, detach, then free outside the lock.
    pub fn trim(self: *SearchResultsPool) void {
        self.mutex.lockUncancelable();
        var idle: ?*SearchResults = null;
        if (self.low_water > 0) {
            const keep = self.free_count - (self.low_water + 1) / 2;
            if (keep == 0) {
                idle = self.free;
                self.free = null;
            } else {
                var last = self.free.?;
                for (1..keep) |_| last = last.pool_next.?;
                idle = last.pool_next;
                last.pool_next = null;
            }
            self.free_count = keep;
        }
        self.low_water = self.free_count;
        self.mutex.unlock();

        while (idle) |r| {
            idle = r.pool_next;
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
                self.free_count -= 1;
                self.low_water = @min(self.low_water, self.free_count);
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
        r.resetForReuse(self.max_retained_hits);
        self.mutex.lockUncancelable();
        defer self.mutex.unlock();
        r.pool_next = self.free;
        self.free = r;
        self.free_count += 1;
    }
};

test "results pool caps the hit map a parked collector may hold" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var pool = SearchResultsPool.init(std.testing.allocator);
    pool.max_retained_hits = 16;
    defer pool.deinit();

    // A normal-sized map is kept, so the next query doesn't have to grow it again.
    const small = try pool.acquire(.{});
    try small.incr(1, 1);
    pool.release(small);
    try std.testing.expect(small.hits.capacity() > 0);

    // A collector whose map ballooned gives the memory back instead of parking it.
    const big = try pool.acquire(.{});
    for (0..1000) |i| try big.incr(@intCast(i + 1), 1);
    try std.testing.expect(big.hits.capacity() > pool.max_retained_hits);
    pool.release(big);
    try std.testing.expectEqual(0, big.hits.capacity());
}

test "results pool grows to peak concurrency and trims back to steady state" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var pool = SearchResultsPool.init(std.testing.allocator);
    defer pool.deinit();

    // A burst of 8 concurrent searches: all of them get pooled, no cap in the way.
    var burst: [8]*SearchResults = undefined;
    for (&burst) |*r| r.* = try pool.acquire(.{});
    for (burst) |r| pool.release(r);
    try std.testing.expectEqual(8, pool.free_count);

    // The sweep that the burst grew into reclaims nothing: the list really was empty
    // partway through it, so there's no evidence yet that any entry is idle. It just
    // sets the baseline.
    pool.trim();
    try std.testing.expectEqual(8, pool.free_count);

    // Now an interval where only 2 are ever in flight. 6 collectors sat untouched;
    // the sweep gives back half of them and leaves the decay to later sweeps.
    useConcurrently(&pool, 2);
    pool.trim();
    try std.testing.expectEqual(5, pool.free_count);

    // Repeated low-usage intervals decay to the working set and stop there - the
    // 2 collectors actually in flight are never idle at sweep time.
    for (0..4) |_| {
        useConcurrently(&pool, 2);
        pool.trim();
    }
    try std.testing.expectEqual(2, pool.free_count);

    // Going fully idle drains the rest, halving each sweep.
    pool.trim();
    try std.testing.expectEqual(1, pool.free_count);
    pool.trim();
    try std.testing.expectEqual(0, pool.free_count);
}

// Simulate an interval with `n` searches in flight at once, repeated a few times so
// the pool sees the same working set more than once.
fn useConcurrently(pool: *SearchResultsPool, comptime n: usize) void {
    for (0..3) |_| {
        var live: [n]*SearchResults = undefined;
        for (&live) |*r| r.* = pool.acquire(.{}) catch unreachable;
        for (live) |r| pool.release(r);
    }
}
