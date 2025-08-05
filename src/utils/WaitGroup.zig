const std = @import("std");

/// A synchronization primitive that allows waiting for a collection of tasks to complete.
/// Similar to Go's sync.WaitGroup, this provides a way to coordinate completion of
/// multiple concurrent operations.
counter: std.atomic.Value(usize),
completion_event: std.Thread.ResetEvent,

const Self = @This();

/// Initialize a new WaitGroup
pub fn init() Self {
    return Self{
        .counter = std.atomic.Value(usize).init(0),
        .completion_event = .{},
    };
}

/// Add delta to the WaitGroup counter.
/// This should be called before starting tasks that will call done().
/// The counter must not go negative.
pub fn add(self: *Self, delta: usize) void {
    while (true) {
        const current_count = self.counter.load(.acquire);

        // Check for overflow before attempting to add
        if (current_count > std.math.maxInt(usize) - delta) {
            @panic("WaitGroup counter overflow");
        }

        const new_count = current_count + delta;

        // Attempt atomic compare-and-swap
        if (self.counter.cmpxchgWeak(current_count, new_count, .acq_rel, .acquire)) |actual| {
            // CAS failed, retry with the actual value we read
            _ = actual;
            continue;
        } else {
            // CAS succeeded, check if we went from 0 to non-zero
            if (current_count == 0 and new_count > 0) {
                // Reset the completion event since we now have work to wait for
                self.completion_event.reset();
            }
            break;
        }
    }
}

/// Decrement the WaitGroup counter by 1.
/// This should be called when a task completes.
/// If the counter reaches 0, any threads waiting on wait() will be unblocked.
pub fn done(self: *Self) void {
    const old_count = self.counter.fetchSub(1, .acq_rel);

    if (old_count == 0) {
        @panic("WaitGroup counter went negative");
    }

    if (old_count == 1) {
        // Counter reached 0, signal completion to all waiters
        self.completion_event.set();
    }
}

/// Wait until the WaitGroup counter reaches 0.
/// This will block until all tasks that were added have called done().
pub fn wait(self: *Self) void {
    // Keep waiting until counter reaches 0
    while (self.counter.load(.acquire) != 0) {
        // Wait for completion event - this will be set when counter reaches 0
        self.completion_event.wait();

        // After waking up, reset the event for potential next wait cycle
        // This handles the case where more tasks might be added after completion
        if (self.counter.load(.acquire) != 0) {
            self.completion_event.reset();
        }
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

test "WaitGroup normal operation test" {
    var wg = WaitGroup.init();

    // Verify the WaitGroup was initialized correctly
    try testing.expect(wg.isComplete());
    try testing.expectEqual(@as(usize, 0), wg.getCount());

    // Add a task and then complete it properly to test normal operation
    wg.add(1);
    try testing.expect(!wg.isComplete());
    wg.done();
    try testing.expect(wg.isComplete());
}
