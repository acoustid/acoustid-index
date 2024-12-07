const std = @import("std");
const m = @import("metrics");

var metrics = m.initializeNoop(Metrics);

const WithIndex = struct { index: []const u8 };

const Metrics = struct {
    searches: m.Counter(u64),
    updates: m.Counter(u64),
    checkpoints: m.Counter(u64),
    memory_segment_merges: m.Counter(u64),
    file_segment_merges: m.Counter(u64),
    docs: m.GaugeVec(u32, WithIndex),
};

pub fn search() void {
    metrics.searches.incr();
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

pub fn initializeMetrics(allocator: std.mem.Allocator, comptime opts: m.RegistryOpts) !void {
    metrics = .{
        .searches = m.Counter(u64).init("searches_total", .{}, opts),
        .updates = m.Counter(u64).init("updates_total", .{}, opts),
        .checkpoints = m.Counter(u64).init("checkpoints_total", .{}, opts),
        .memory_segment_merges = m.Counter(u64).init("memory_segment_merges_total", .{}, opts),
        .file_segment_merges = m.Counter(u64).init("file_segment_merges_total", .{}, opts),
        .docs = try m.GaugeVec(u32, WithIndex).init(allocator, "docs", .{}, opts),
    };
}

pub fn writeMetrics(writer: anytype) !void {
    return m.write(&metrics, writer);
}
