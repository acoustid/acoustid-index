const std = @import("std");
const m = @import("metrics");

var metrics = m.initializeNoop(Metrics);
var arena: ?std.heap.ArenaAllocator = null;

const WithIndex = struct { index: []const u8 };

const SearchDuration = m.Histogram(
    f64,
    &.{ 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10 },
);

const StartupDuration = m.Histogram(
    f64,
    &.{ 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10 },
);

const ParallelSegmentCount = m.Histogram(
    u64,
    &.{ 1, 2, 3, 5, 10, 20, 50, 100 },
);

const ScannedDocsPerHash = m.Histogram(
    u64,
    &.{ 1, 2, 3, 5, 10, 50, 100, 500, 1000 },
);

const ScannedBlocksPerHash = m.Histogram(
    u64,
    &.{ 1, 2, 3, 5, 10 },
);

const Metrics = struct {
    search_hits: m.Counter(u64),
    search_misses: m.Counter(u64),
    search_duration: SearchDuration,
    searches: m.Counter(u64),
    updates: m.Counter(u64),
    checkpoints: m.Counter(u64),
    memory_segment_merges: m.Counter(u64),
    file_segment_merges: m.Counter(u64),
    docs: m.GaugeVec(u32, WithIndex),
    scanned_docs_per_hash: ScannedDocsPerHash,
    scanned_blocks_per_hash: ScannedBlocksPerHash,
    // Parallel loading metrics
    parallel_loading_total: m.Counter(u64),
    sequential_loading_total: m.Counter(u64),
    startup_duration: StartupDuration,
    parallel_segment_count: ParallelSegmentCount,
};

pub fn search() void {
    metrics.searches.incr();
}

pub fn searchHit() void {
    metrics.search_hits.incr();
}

pub fn searchMiss() void {
    metrics.search_misses.incr();
}

pub fn searchDuration(duration_ms: i64) void {
    metrics.search_duration.observe(@as(f64, @floatFromInt(duration_ms)) / 1000.0);
}

pub fn scannedDocsPerHash(num_docs: u64) void {
    metrics.scanned_docs_per_hash.observe(num_docs);
}

pub fn scannedBlocksPerHash(num_blocks: u64) void {
    metrics.scanned_blocks_per_hash.observe(num_blocks);
}

pub fn update(count: usize) void {
    metrics.updates.incrBy(@intCast(count));
}

pub fn checkpoint() void {
    metrics.checkpoints.incr();
}

pub fn memorySegmentMerge() void {
    metrics.memory_segment_merges.incr();
}

pub fn fileSegmentMerge() void {
    metrics.file_segment_merges.incr();
}

pub fn docs(index_name: []const u8, value: u32) void {
    metrics.docs.set(.{ .index = index_name }, value) catch {};
}

pub fn parallelLoading(segment_count: usize) void {
    metrics.parallel_loading_total.incr();
    metrics.parallel_segment_count.observe(@intCast(segment_count));
}

pub fn sequentialLoading() void {
    metrics.sequential_loading_total.incr();
}

pub fn startupDuration(duration_ms: i64) void {
    metrics.startup_duration.observe(@as(f64, @floatFromInt(duration_ms)) / 1000.0);
}

pub fn initializeMetrics(allocator: std.mem.Allocator, comptime opts: m.RegistryOpts) !void {
    arena = std.heap.ArenaAllocator.init(allocator);
    const alloc = arena.?.allocator();

    metrics = .{
        .search_hits = m.Counter(u64).init("search_hits_total", .{}, opts),
        .search_misses = m.Counter(u64).init("search_misses_total", .{}, opts),
        .search_duration = SearchDuration.init("search_duration_seconds", .{}, opts),
        .searches = m.Counter(u64).init("searches_total", .{}, opts),
        .updates = m.Counter(u64).init("updates_total", .{}, opts),
        .checkpoints = m.Counter(u64).init("checkpoints_total", .{}, opts),
        .memory_segment_merges = m.Counter(u64).init("memory_segment_merges_total", .{}, opts),
        .file_segment_merges = m.Counter(u64).init("file_segment_merges_total", .{}, opts),
        .docs = try m.GaugeVec(u32, WithIndex).init(alloc, "docs", .{}, opts),
        .scanned_docs_per_hash = ScannedDocsPerHash.init("scanned_docs_per_hash", .{}, opts),
        .scanned_blocks_per_hash = ScannedBlocksPerHash.init("scanned_blocks_per_hash", .{}, opts),
        // Initialize parallel loading metrics
        .parallel_loading_total = m.Counter(u64).init("parallel_loading_total", .{}, opts),
        .sequential_loading_total = m.Counter(u64).init("sequential_loading_total", .{}, opts),
        .startup_duration = StartupDuration.init("startup_duration_seconds", .{}, opts),
        .parallel_segment_count = ParallelSegmentCount.init("parallel_segment_count", .{}, opts),
    };
}

pub fn deinitMetrics() void {
    arena.?.deinit();
}

pub fn writeMetrics(writer: anytype) !void {
    return m.write(&metrics, writer);
}
