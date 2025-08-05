const std = @import("std");

/// A synchronization primitive that allows waiting for a collection of tasks to complete.
/// Similar to Go's sync.WaitGroup, this provides a way to coordinate completion of
/// multiple concurrent operations.
counter: std.atomic.Value(u32),
mutex: std.Thread.Mutex,
condition: std.Thread.Condition,

const Self = @This();

/// Initialize a new WaitGroup
pub fn init() Self {
    return Self{
        .counter = std.atomic.Value(u32).init(0),
        .mutex = .{},
        .condition = .{},
    };
}

/// Add delta to the WaitGroup counter.
/// This should be called before starting tasks that will call done().
/// The counter must not go negative.
pub fn add(self: *Self, delta: usize) void {
    if (delta > std.math.maxInt(u32)) {
        @panic("WaitGroup delta too large");
    }
    
    const delta_u32 = @as(u32, @intCast(delta));
    const old_counter = self.counter.fetchAdd(delta_u32, .acq_rel);
    
    // Check for overflow
    if (old_counter > std.math.maxInt(u32) - delta_u32) {
        @panic("WaitGroup counter overflow");
    }
}

/// Decrement the WaitGroup counter by 1.
/// This should be called when a task completes.
/// If the counter reaches 0, any threads waiting on wait() will be unblocked.
pub fn done(self: *Self) void {
    const old_counter = self.counter.fetchSub(1, .acq_rel);
    
    if (old_counter == 0) {
        @panic("WaitGroup counter went negative");
    }
    
    if (old_counter == 1) {
        // Counter reached 0, wake all waiters
        self.mutex.lock();
        defer self.mutex.unlock();
        self.condition.broadcast();
    }
}

/// Wait until the WaitGroup counter reaches 0.
/// This will block until all tasks that were added have called done().
pub fn wait(self: *Self) void {
    self.mutex.lock();
    defer self.mutex.unlock();
    
    while (self.counter.load(.acquire) != 0) {
        self.condition.wait(&self.mutex);
    }
}

/// Check if all tasks are complete without blocking.
/// Returns true if the counter is 0.
pub fn isComplete(self: *Self) bool {
    return self.counter.load(.acquire) == 0;
}

/// Get the current count of pending tasks.
/// This is primarily useful for debugging and monitoring.
pub fn getCount(self: *Self) usize {
    return self.counter.load(.acquire);
}

// Tests
const testing = std.testing;

const WaitGroup = @This();

test "WaitGroup basic functionality" {
    var wg = WaitGroup.init();

    // Initially should be complete
    try testing.expect(wg.isComplete());
    try testing.expectEqual(@as(usize, 0), wg.getCount());

    // Add some work
    wg.add(3);
    try testing.expect(!wg.isComplete());
    try testing.expectEqual(@as(usize, 3), wg.getCount());

    // Complete work one by one
    wg.done();
    try testing.expectEqual(@as(usize, 2), wg.getCount());

    wg.done();
    try testing.expectEqual(@as(usize, 1), wg.getCount());

    wg.done();
    try testing.expect(wg.isComplete());
    try testing.expectEqual(@as(usize, 0), wg.getCount());
}

test "WaitGroup with threading" {
    var wg = WaitGroup.init();
    const num_threads = 4;

    wg.add(num_threads);

    const WorkerData = struct {
        wg: *WaitGroup,
        delay_ms: u64,
    };

    const worker = struct {
        fn run(data: WorkerData) void {
            std.time.sleep(data.delay_ms * std.time.ns_per_ms);
            data.wg.done();
        }
    };

    var threads: [num_threads]std.Thread = undefined;

    // Start threads
    for (&threads, 0..) |*thread, i| {
        const data = WorkerData{ .wg = &wg, .delay_ms = (i + 1) * 10 };
        thread.* = try std.Thread.spawn(.{}, worker.run, .{data});
    }

    // Wait for all to complete
    wg.wait();

    // Join threads
    for (&threads) |*thread| {
        thread.join();
    }

    try testing.expect(wg.isComplete());
}

test "WaitGroup multiple add/done cycles" {
    var wg = WaitGroup.init();

    // First cycle: add multiple tasks, complete them all
    wg.add(3);
    try testing.expectEqual(@as(usize, 3), wg.getCount());
    
    wg.done();
    wg.done(); 
    wg.done();
    try testing.expect(wg.isComplete());

    // Second cycle: add more tasks after completion
    wg.add(2);
    try testing.expectEqual(@as(usize, 2), wg.getCount());
    try testing.expect(!wg.isComplete());
    
    wg.done();
    wg.done();
    try testing.expect(wg.isComplete());

    // Third cycle: add single task
    wg.add(1);
    try testing.expect(!wg.isComplete());
    wg.done();
    try testing.expect(wg.isComplete());
}
