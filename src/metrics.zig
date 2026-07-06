// Global process metrics, backed by karlseguin/metrics.zig (Prometheus). Global
// counters + a search-duration histogram live here; per-index gauges are
// rendered on demand by MultiIndex (it holds the indexes).

const std = @import("std");
const m = @import("metrics");

const SearchDuration = m.Histogram(f64, &.{ 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5 });
const ScannedDocs = m.Histogram(u64, &.{ 1, 2, 3, 5, 10, 50, 100, 500, 1000 });
const ScannedBlocks = m.Histogram(u64, &.{ 1, 2, 3, 5, 10 });

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
};

// No-op until init(); calls before then just don't record (never crash).
var metrics = m.initializeNoop(Metrics);

/// Wire up real metrics. Call once at startup. No allocation — counters and
/// histograms are inline (only labelled vecs would need an allocator).
pub fn init(comptime opts: m.RegistryOpts) void {
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
    };
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

/// Render the global metrics in Prometheus text format.
pub fn writeGlobal(w: *std.Io.Writer) !void {
    return m.write(&metrics, w);
}
