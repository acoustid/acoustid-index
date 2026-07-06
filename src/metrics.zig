// Global process metrics, backed by karlseguin/metrics.zig (Prometheus). Global
// counters + a search-duration histogram live here; per-index gauges are
// rendered on demand by MultiIndex (it holds the indexes).

const std = @import("std");
const m = @import("metrics");

const SearchDuration = m.Histogram(f64, &.{ 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5 });

const Metrics = struct {
    searches: m.Counter(u64),
    updates: m.Counter(u64),
    checkpoints: m.Counter(u64),
    memory_merges: m.Counter(u64),
    file_merges: m.Counter(u64),
    search_duration: SearchDuration,
};

// No-op until init(); calls before then just don't record (never crash).
var metrics = m.initializeNoop(Metrics);

/// Wire up real metrics. Call once at startup. No allocation — counters and
/// histograms are inline (only labelled vecs would need an allocator).
pub fn init(comptime opts: m.RegistryOpts) void {
    metrics = .{
        .searches = m.Counter(u64).init("fpindex_searches_total", .{}, opts),
        .updates = m.Counter(u64).init("fpindex_updates_total", .{}, opts),
        .checkpoints = m.Counter(u64).init("fpindex_checkpoints_total", .{}, opts),
        .memory_merges = m.Counter(u64).init("fpindex_memory_merges_total", .{}, opts),
        .file_merges = m.Counter(u64).init("fpindex_file_merges_total", .{}, opts),
        .search_duration = SearchDuration.init("fpindex_search_duration_seconds", .{}, opts),
    };
}

pub fn incSearches() void {
    metrics.searches.incr();
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

/// Render the global metrics in Prometheus text format.
pub fn writeGlobal(w: *std.Io.Writer) !void {
    return m.write(&metrics, w);
}
