const std = @import("std");
const log = std.log.scoped(.scheduler);

pub const Task = struct {
    reschedule: usize = 0,
    scheduled: bool = false,
    running: bool = false,
    done: std.Thread.ResetEvent = .{},
    ctx: *anyopaque,
    runFn: *const fn (ctx: *anyopaque) void,
    deinitFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator) void,
    interval_ns: ?u64 = null,
    next_run_time_ns: u64,
    /// When true, task is automatically destroyed after first execution.
    /// One-shot tasks must not be referenced after scheduling via runOnce().
    one_shot: bool = false,
};

const TaskQueue = std.PriorityQueue(*Task, void, compareTasksByDeadline);

fn compareTasksByDeadline(context: void, a: *Task, b: *Task) std.math.Order {
    _ = context;
    // Both tasks should have next_run_time_ns set (immediate tasks get current time)
    const a_time = a.next_run_time_ns;
    const b_time = b.next_run_time_ns;

    // Primary ordering by timestamp
    const time_order = std.math.order(a_time, b_time);
    if (time_order != .eq) return time_order;

    // Secondary ordering by pointer address for absolute order
    return std.math.order(@intFromPtr(a), @intFromPtr(b));
}

const Self = @This();

allocator: std.mem.Allocator,
threads: std.ArrayListUnmanaged(std.Thread) = .{},

queue: TaskQueue,
queue_not_empty: std.Thread.Condition = .{},
queue_mutex: std.Thread.Mutex = .{},
stopping: bool = false,

// Monotonic timer for precise scheduling
timer: std.time.Timer = undefined,
num_tasks: usize = 0,

pub fn init(allocator: std.mem.Allocator) Self {
    return .{
        .allocator = allocator,
        .queue = TaskQueue.init(allocator, {}),
        .timer = std.time.Timer.start() catch unreachable, // Start monotonic timer
    };
}

pub fn deinit(self: *Self) void {
    self.stop();
    self.threads.deinit(self.allocator);
    self.queue.deinit();

    if (self.num_tasks > 0) {
        log.err("still have {} active tasks", .{self.num_tasks});
        std.debug.assert(self.num_tasks == 0);
    }
}

pub fn createTask(self: *Self, comptime func: anytype, args: anytype) !*Task {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    const task = try self.allocator.create(Task);
    errdefer self.allocator.destroy(task);

    const Args = @TypeOf(args);
    const Closure = struct {
        arguments: Args,

        fn deinit(ctx: *anyopaque, allocator: std.mem.Allocator) void {
            const closure: *@This() = @ptrCast(@alignCast(ctx));
            allocator.destroy(closure);
        }

        fn run(ctx: *anyopaque) void {
            const closure: *@This() = @ptrCast(@alignCast(ctx));
            @call(.auto, func, closure.arguments);
        }
    };

    const closure = try self.allocator.create(Closure);
    errdefer self.allocator.destroy(closure);

    closure.arguments = args;

    task.* = .{
        .ctx = closure,
        .runFn = Closure.run,
        .deinitFn = Closure.deinit,
        .next_run_time_ns = 0, // Will be set when scheduled
    };
    task.done.set();

    self.num_tasks += 1;

    return task;
}

pub fn createRepeatingTask(self: *Self, interval_ms: u32, comptime func: anytype, args: anytype) !*Task {
    const task = try self.createTask(func, args);
    task.interval_ns = @as(u64, interval_ms) * std.time.ns_per_ms;
    return task;
}

/// Schedules a fire-and-forget task that automatically destroys itself after execution.
/// The task pointer must not be accessed after calling this function, as it will be
/// freed automatically when the task completes.
pub fn runOnce(self: *Self, comptime func: anytype, args: anytype) !void {
    const task = try self.createTask(func, args);
    task.one_shot = true;
    self.scheduleTask(task);
}

fn removeFromQueue(self: *Self, task: *Task) void {
    if (std.mem.indexOfScalar(*Task, self.queue.items, task)) |index| {
        _ = self.queue.removeIndex(index);
        task.scheduled = false;
        task.next_run_time_ns = 0; // Reset to immediate so it can be re-enqueued
    }
}

fn dequeue(self: *Self, task: *Task) void {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    if (task.scheduled) {
        self.removeFromQueue(task);
    }

    task.reschedule = 0;
}

pub fn destroyTask(self: *Self, task: *Task) void {
    self.dequeue(task);

    task.done.wait();

    task.deinitFn(task.ctx, self.allocator);
    self.allocator.destroy(task);

    std.debug.assert(self.num_tasks > 0);
    self.num_tasks -= 1;
}

pub fn scheduleTask(self: *Self, task: *Task) void {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    if (task.scheduled or task.running) {
        task.reschedule += 1;
    } else {
        self.enqueue(task);
    }
}

pub fn scheduleTaskAfter(self: *Self, task: *Task, delay_ms: u32) void {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    // Use monotonic time from scheduler timer
    const current_time_ns = self.timer.read();
    const delay_ns = @as(u64, delay_ms) * std.time.ns_per_ms;
    const run_time_ns = current_time_ns + delay_ns;

    if (task.running) {
        task.reschedule += 1;
        task.next_run_time_ns = run_time_ns;
        return;
    }

    if (task.scheduled) {
        self.removeFromQueue(task);
    }

    task.next_run_time_ns = run_time_ns;
    self.enqueue(task);
}

pub fn cancelRepeatingTask(self: *Self, task: *Task) void {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    task.interval_ns = null;

    // If queued and not running, unschedule immediately
    if (task.scheduled and !task.running) {
        self.removeFromQueue(task);
    }

    // Clear any pending reschedules and complete if nothing pending
    task.reschedule = 0;
    if (!task.running) {
        task.done.set();
    }
}

fn getNextDeadline(self: *Self) ?u64 {
    return if (self.queue.peek()) |task| task.next_run_time_ns else null;
}

fn enqueue(self: *Self, task: *Task) void {
    task.scheduled = true;

    // Treat immediate tasks as "scheduled now"
    if (task.next_run_time_ns == 0) {
        task.next_run_time_ns = self.timer.read();
    }

    self.queue.add(task) catch |err| {
        log.err("failed to add task to queue: {}", .{err});
        // Fallback: mark as not scheduled and signal done
        task.scheduled = false;
        task.done.set();
        // If one-shot, free immediately to avoid leaks and num_tasks imbalance.
        if (task.one_shot) {
            task.deinitFn(task.ctx, self.allocator);
            self.allocator.destroy(task);
            std.debug.assert(self.num_tasks > 0);
            self.num_tasks -= 1;
        }
        return;
    };

    self.queue_not_empty.signal();
}

fn getTaskToRun(self: *Self) ?*Task {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    while (!self.stopping) {
        const current_time_ns = self.timer.read();

        // O(1) peek at next task + O(log n) removal if ready
        if (self.queue.peek()) |task| {
            const task_time_ns = task.next_run_time_ns;
            if (current_time_ns >= task_time_ns) {
                const removed_task = self.queue.remove();
                removed_task.scheduled = false;
                removed_task.running = true;
                removed_task.done.reset();

                return removed_task;
            }
        }

        // Calculate timeout using next deadline (O(1) peek)
        const timeout_ns: u64 = if (self.getNextDeadline()) |deadline| blk: {
            // If deadline has passed, use 0 timeout; otherwise use remaining time
            break :blk if (current_time_ns >= deadline) 0 else deadline - current_time_ns;
        } else std.time.ns_per_min;

        self.queue_not_empty.timedWait(&self.queue_mutex, timeout_ns) catch {};
    }
    return null;
}

/// Calculate next run time for repeating tasks using fixed-delay scheduling
/// (interval from now rather than from original deadline)
fn nextRunAfterInterval(self: *Self, task: *Task) u64 {
    const interval = task.interval_ns.?;
    return self.timer.read() + interval;
}

fn markAsDone(self: *Self, task: *Task) void {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    const has_manual_reschedule = task.reschedule > 0;
    const is_repeating = task.interval_ns != null;

    if (task.one_shot) {
        // Auto-destroy one-shot tasks
        task.running = false;
        task.done.set(); // Signal completion before cleanup
        task.deinitFn(task.ctx, self.allocator);
        self.allocator.destroy(task);
        std.debug.assert(self.num_tasks > 0);
        self.num_tasks -= 1;
    } else if (has_manual_reschedule or is_repeating) {
        if (has_manual_reschedule) {
            task.reschedule -= 1;
        }

        if (is_repeating) {
            // If no manual absolute time was set during execution, use interval
            if (!has_manual_reschedule) {
                task.next_run_time_ns = self.nextRunAfterInterval(task);
            }
        }

        task.running = false;
        self.enqueue(task);
    } else {
        task.running = false;
        task.done.set();
    }
}

fn workerThreadFunc(self: *Self) void {
    while (true) {
        const task = self.getTaskToRun() orelse break;
        defer self.markAsDone(task);

        task.runFn(task.ctx);
    }
}

pub fn start(self: *Self, thread_count: usize) !void {
    errdefer self.stop();

    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    self.stopping = false;

    try self.threads.ensureUnusedCapacity(self.allocator, thread_count);
    for (0..thread_count) |_| {
        const thread = try std.Thread.spawn(.{}, workerThreadFunc, .{self});
        self.threads.appendAssumeCapacity(thread);
    }

    log.info("started {} worker threads", .{thread_count});
}

pub fn stop(self: *Self) void {
    {
        self.queue_mutex.lock();
        defer self.queue_mutex.unlock();

        self.stopping = true;
        self.queue_not_empty.broadcast();
    }

    for (self.threads.items) |*thread| {
        thread.join();
    }
    self.threads.clearRetainingCapacity();
}

test "Scheduler: smoke test" {
    var scheduler = Self.init(std.testing.allocator);
    defer scheduler.deinit();

    const Counter = struct {
        count: usize = 0,

        fn incr(self: *@This()) void {
            self.count += 1;
        }
    };
    var counter: Counter = .{};

    const task = try scheduler.createTask(Counter.incr, .{&counter});
    defer scheduler.destroyTask(task);

    for (0..3) |_| {
        scheduler.scheduleTask(task);
    }

    try scheduler.start(2);
    std.time.sleep(std.time.us_per_s);
    scheduler.stop();

    try std.testing.expect(counter.count == 3);
}

test "Scheduler: repeating task" {
    var scheduler = Self.init(std.testing.allocator);
    defer scheduler.deinit();

    const Counter = struct {
        count: usize = 0,
        max_count: usize = 5,

        fn incr(self: *@This()) void {
            self.count += 1;
        }
    };
    var counter: Counter = .{};

    const task = try scheduler.createRepeatingTask(100, Counter.incr, .{&counter});
    defer scheduler.destroyTask(task);

    scheduler.scheduleTask(task);

    try scheduler.start(1);
    std.time.sleep(600 * std.time.ns_per_ms);
    scheduler.cancelRepeatingTask(task);
    std.time.sleep(200 * std.time.ns_per_ms);
    scheduler.stop();

    try std.testing.expect(counter.count >= 5);
    try std.testing.expect(counter.count <= 7);
}

test "Scheduler: scheduled task with delay" {
    var scheduler = Self.init(std.testing.allocator);
    defer scheduler.deinit();

    const Counter = struct {
        count: usize = 0,
        start_time_ns: u64,
        scheduler_timer: *std.time.Timer,

        fn incr(self: *@This()) void {
            const current_time_ns = self.scheduler_timer.read();
            const elapsed_ns = current_time_ns - self.start_time_ns;
            if (elapsed_ns >= 200 * std.time.ns_per_ms) {
                self.count += 1;
            }
        }
    };
    const start_time_ns = scheduler.timer.read();
    var counter: Counter = .{ .start_time_ns = start_time_ns, .scheduler_timer = &scheduler.timer };

    const task = try scheduler.createTask(Counter.incr, .{&counter});
    defer scheduler.destroyTask(task);

    scheduler.scheduleTaskAfter(task, 200); // 200ms delay

    try scheduler.start(1);
    std.time.sleep(300 * std.time.ns_per_ms);
    scheduler.stop();

    try std.testing.expect(counter.count == 1);
}

test "Scheduler: multiple repeating tasks with different intervals" {
    var scheduler = Self.init(std.testing.allocator);
    defer scheduler.deinit();

    const Counter = struct {
        fast_count: usize = 0,
        slow_count: usize = 0,

        fn incrFast(self: *@This()) void {
            self.fast_count += 1;
        }

        fn incrSlow(self: *@This()) void {
            self.slow_count += 1;
        }
    };
    var counter: Counter = .{};

    const fast_task = try scheduler.createRepeatingTask(50, Counter.incrFast, .{&counter});
    defer scheduler.destroyTask(fast_task);

    const slow_task = try scheduler.createRepeatingTask(150, Counter.incrSlow, .{&counter});
    defer scheduler.destroyTask(slow_task);

    scheduler.scheduleTask(fast_task);
    scheduler.scheduleTask(slow_task);

    try scheduler.start(2);
    std.time.sleep(400 * std.time.ns_per_ms);
    scheduler.cancelRepeatingTask(fast_task);
    scheduler.cancelRepeatingTask(slow_task);
    scheduler.stop();

    try std.testing.expect(counter.fast_count >= 6);
    try std.testing.expect(counter.slow_count >= 2);
    try std.testing.expect(counter.fast_count > counter.slow_count);
}

test "Scheduler: runOnce auto-destroys task" {
    var scheduler = Self.init(std.testing.allocator);
    defer scheduler.deinit();

    const Counter = struct {
        count: usize = 0,

        fn incr(self: *@This()) void {
            self.count += 1;
        }
    };
    var counter: Counter = .{};

    const initial_tasks = scheduler.num_tasks;

    // Schedule multiple one-shot tasks
    try scheduler.runOnce(Counter.incr, .{&counter});
    try scheduler.runOnce(Counter.incr, .{&counter});
    try scheduler.runOnce(Counter.incr, .{&counter});

    // Start scheduler and wait for completion with polling
    try scheduler.start(2);
    var waited_ns: u64 = 0;
    while (waited_ns < 500 * std.time.ns_per_ms and
           (counter.count != 3 or scheduler.num_tasks != initial_tasks)) {
        std.time.sleep(10 * std.time.ns_per_ms);
        waited_ns += 10 * std.time.ns_per_ms;
    }
    scheduler.stop();

    // Verify all tasks executed and were auto-destroyed
    try std.testing.expect(counter.count == 3);
    try std.testing.expect(scheduler.num_tasks == initial_tasks);
}
