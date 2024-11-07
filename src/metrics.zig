const m = @import("metrics");

var metrics = m.initializeNoop(Metrics);

const Metrics = struct {
    searches: m.Counter(u32),
    updates: m.Counter(u32),
};

pub fn search() void {
    metrics.searches.incr();
}

pub fn update(count: usize) void {
    metrics.updates.incrBy(@intCast(count));
}

pub fn initializeMetrics(comptime opts: m.RegistryOpts) !void {
    metrics = .{
        .searches = m.Counter(u32).init("searches_total", .{}, opts),
        .updates = m.Counter(u32).init("updates_total", .{}, opts),
    };
}

pub fn writeMetrics(writer: anytype) !void {
    return m.write(&metrics, writer);
}
