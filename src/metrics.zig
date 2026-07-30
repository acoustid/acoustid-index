// Global process metrics, backed by karlseguin/metrics.zig (Prometheus).
// Counters and histograms are recorded as events happen; the per-index gauges
// are refreshed from the live indexes at scrape time (MultiIndex.writeMetrics)
// and removed when an index is dropped.

const std = @import("std");
const m = @import("metrics");

const SearchDuration = m.Histogram(f64, &.{ 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5 });
const ScannedDocs = m.Histogram(u64, &.{ 1, 2, 3, 5, 10, 50, 100, 500, 1000 });
const ScannedBlocks = m.Histogram(u64, &.{ 1, 2, 3, 5, 10 });

const IndexLabels = struct { index: []const u8 };
const IndexGauge = m.GaugeVec(u64, IndexLabels);

const Metrics = struct {
    searches: m.Counter(u64),
    search_hits: m.Counter(u64),
    search_misses: m.Counter(u64),
    updates: m.Counter(u64),
    checkpoints: m.Counter(u64),
    memory_merges: m.Counter(u64),
    file_merges: m.Counter(u64),
    search_duration: SearchDuration,
    scanned_docs_per_hash: ScannedDocs,
    scanned_blocks_per_hash: ScannedBlocks,
    docs: IndexGauge,
    version: IndexGauge,
};

// No-op until init(); calls before then just don't record (never crash).
var metrics = m.initializeNoop(Metrics);

/// Wire up real metrics. Call once at startup. The allocator and io are only
/// used by the labelled vecs (label sets are duplicated into the allocator).
pub fn init(allocator: std.mem.Allocator, io: std.Io, comptime opts: m.RegistryOpts) !void {
    metrics = .{
        .searches = m.Counter(u64).init("fpindex_searches_total", .{}, opts),
        .search_hits = m.Counter(u64).init("fpindex_search_hits_total", .{}, opts),
        .search_misses = m.Counter(u64).init("fpindex_search_misses_total", .{}, opts),
        .updates = m.Counter(u64).init("fpindex_updates_total", .{}, opts),
        .checkpoints = m.Counter(u64).init("fpindex_checkpoints_total", .{}, opts),
        .memory_merges = m.Counter(u64).init("fpindex_memory_merges_total", .{}, opts),
        .file_merges = m.Counter(u64).init("fpindex_file_merges_total", .{}, opts),
        .search_duration = SearchDuration.init("fpindex_search_duration_seconds", .{}, opts),
        .scanned_docs_per_hash = ScannedDocs.init("fpindex_scanned_docs_per_hash", .{}, opts),
        .scanned_blocks_per_hash = ScannedBlocks.init("fpindex_scanned_blocks_per_hash", .{}, opts),
        .docs = try IndexGauge.init(allocator, io, "fpindex_docs", .{ .help = "Number of documents in an index" }, opts),
        .version = try IndexGauge.init(allocator, io, "fpindex_version", .{ .help = "Upstream changelog position the index reflects" }, opts),
    };
}

pub fn deinit() void {
    metrics.docs.deinit();
    metrics.version.deinit();
    metrics = m.initializeNoop(Metrics);
}

pub fn incSearches() void {
    metrics.searches.incr();
}
pub fn incSearchHit() void {
    metrics.search_hits.incr();
}
pub fn incSearchMiss() void {
    metrics.search_misses.incr();
}
pub fn incUpdates() void {
    metrics.updates.incr();
}
pub fn incCheckpoints() void {
    metrics.checkpoints.incr();
}
pub fn incMemoryMerges() void {
    metrics.memory_merges.incr();
}
pub fn incFileMerges() void {
    metrics.file_merges.incr();
}
pub fn observeSearchSeconds(seconds: f64) void {
    metrics.search_duration.observe(seconds);
}
pub fn observeScannedDocsPerHash(n: u64) void {
    metrics.scanned_docs_per_hash.observe(n);
}
pub fn observeScannedBlocksPerHash(n: u64) void {
    metrics.scanned_blocks_per_hash.observe(n);
}

pub fn setDocs(index_name: []const u8, n: u64) !void {
    try metrics.docs.set(.{ .index = index_name }, n);
}
pub fn setVersion(index_name: []const u8, v: u64) !void {
    try metrics.version.set(.{ .index = index_name }, v);
}
/// Drop all per-index series for a deleted index; without this the stale
/// values would be scraped until restart.
pub fn removeIndex(index_name: []const u8) void {
    metrics.docs.remove(.{ .index = index_name });
    metrics.version.remove(.{ .index = index_name });
}

/// Render all metrics in Prometheus text format. Callers who need the
/// per-index gauges fresh must set them first (see MultiIndex.writeMetrics).
pub fn write(w: *std.Io.Writer) !void {
    return m.write(&metrics, w);
}
