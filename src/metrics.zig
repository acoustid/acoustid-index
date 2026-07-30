// Global process metrics, backed by karlseguin/metrics.zig (Prometheus).
// Counters and histograms are recorded as events happen; the per-index gauges
// are refreshed from the live indexes at scrape time (MultiIndex.writeMetrics)
// and removed when an index is dropped.

const std = @import("std");
const m = @import("metrics");

const ScannedDocs = m.Histogram(u64, &.{ 1, 2, 3, 5, 10, 50, 100, 500, 1000 });
const ScannedBlocks = m.Histogram(u64, &.{ 1, 2, 3, 5, 10 });

const IndexLabels = struct { index: []const u8 };
const IndexCounter = m.CounterVec(u64, IndexLabels);
const IndexGauge = m.GaugeVec(u64, IndexLabels);
const SearchDuration = m.HistogramVec(f64, IndexLabels, &.{ 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5 });

// The request-level metrics are labelled per index (MultiIndex knows the name);
// the segment- and background-level ones (scans, checkpoints, merges) stay
// global — those layers don't know the index name and per-index resolution
// isn't worth threading it down.
const Metrics = struct {
    searches: IndexCounter,
    search_hits: IndexCounter,
    search_misses: IndexCounter,
    updates: IndexCounter,
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
        .searches = try IndexCounter.init(allocator, io, "fpindex_searches_total", .{}, opts),
        .search_hits = try IndexCounter.init(allocator, io, "fpindex_search_hits_total", .{}, opts),
        .search_misses = try IndexCounter.init(allocator, io, "fpindex_search_misses_total", .{}, opts),
        .updates = try IndexCounter.init(allocator, io, "fpindex_updates_total", .{}, opts),
        .checkpoints = m.Counter(u64).init("fpindex_checkpoints_total", .{}, opts),
        .memory_merges = m.Counter(u64).init("fpindex_memory_merges_total", .{}, opts),
        .file_merges = m.Counter(u64).init("fpindex_file_merges_total", .{}, opts),
        .search_duration = try SearchDuration.init(allocator, io, "fpindex_search_duration_seconds", .{}, opts),
        .scanned_docs_per_hash = ScannedDocs.init("fpindex_scanned_docs_per_hash", .{}, opts),
        .scanned_blocks_per_hash = ScannedBlocks.init("fpindex_scanned_blocks_per_hash", .{}, opts),
        .docs = try IndexGauge.init(allocator, io, "fpindex_docs", .{ .help = "Number of documents in an index" }, opts),
        .version = try IndexGauge.init(allocator, io, "fpindex_version", .{ .help = "Upstream changelog position the index reflects" }, opts),
    };
}

pub fn deinit() void {
    metrics.searches.deinit();
    metrics.search_hits.deinit();
    metrics.search_misses.deinit();
    metrics.updates.deinit();
    metrics.search_duration.deinit();
    metrics.docs.deinit();
    metrics.version.deinit();
    metrics = m.initializeNoop(Metrics);
}

// The labelled increments can fail (first sight of a label allocates), but a
// metrics allocation failure must not fail the search/update it measures, so
// they swallow the error — the sample is dropped, the operation proceeds.
pub fn incSearches(index_name: []const u8) void {
    metrics.searches.incr(.{ .index = index_name }) catch {};
}
pub fn incSearchHit(index_name: []const u8) void {
    metrics.search_hits.incr(.{ .index = index_name }) catch {};
}
pub fn incSearchMiss(index_name: []const u8) void {
    metrics.search_misses.incr(.{ .index = index_name }) catch {};
}
pub fn incUpdates(index_name: []const u8) void {
    metrics.updates.incr(.{ .index = index_name }) catch {};
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
pub fn observeSearchSeconds(index_name: []const u8, seconds: f64) void {
    metrics.search_duration.observe(.{ .index = index_name }, seconds) catch {};
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
/// values would be scraped until restart. HistogramVec has no remove(), so a
/// deleted index's search_duration series lingers frozen — harmless (its rate
/// reads as zero), and a recreated index simply resumes it.
pub fn removeIndex(index_name: []const u8) void {
    const labels: IndexLabels = .{ .index = index_name };
    metrics.searches.remove(labels);
    metrics.search_hits.remove(labels);
    metrics.search_misses.remove(labels);
    metrics.updates.remove(labels);
    metrics.docs.remove(labels);
    metrics.version.remove(labels);
}

/// Render all metrics in Prometheus text format. Callers who need the
/// per-index gauges fresh must set them first (see MultiIndex.writeMetrics).
pub fn write(w: *std.Io.Writer) !void {
    return m.write(&metrics, w);
}
