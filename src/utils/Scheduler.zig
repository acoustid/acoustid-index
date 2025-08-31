const std = @import("std");
const log = std.log.scoped(.scheduler);

const Priority = enum(u8) {
    high = 0,
    medium = 1,
    low = 2,
    do_not_run = 3,
};

const TaskStatus = struct {
    reschedule: usize = 0,
    scheduled: bool = false,
    running: bool = false,
    done: std.Thread.ResetEvent = .{},
    priority: Priority,
    ctx: *anyopaque,
    runFn: *const fn (ctx: *anyopaque) void,
    deinitFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator) void,
    interval_ns: ?u64 = null,
    next_run_time_ns: ?u64 = null,
    sequence: u64 = 0,  // For absolute ordering when timestamps are equal
};

const TaskData = struct {
    task_status: TaskStatus,
    // No need for next/prev pointers - priority queue handles ordering
};

pub const Task = *TaskData;

const TaskQueue = std.PriorityQueue(*TaskData, void, compareTasksByDeadline);

fn compareTasksByDeadline(context: void, a: *TaskData, b: *TaskData) std.math.Order {
    _ = context;
    // Both tasks should have next_run_time_ns set (immediate tasks get current time)
    const a_time = a.task_status.next_run_time_ns orelse unreachable;
    const b_time = b.task_status.next_run_time_ns orelse unreachable;
    
    // Primary ordering by timestamp
    const time_order = std.math.order(a_time, b_time);
    if (time_order != .eq) return time_order;
    
    // Secondary ordering by sequence for absolute order
    return std.math.order(a.task_status.sequence, b.task_status.sequence);
}

const Self = @This();

allocator: std.mem.Allocator,
threads: std.ArrayListUnmanaged(std.Thread) = .{},

task_queue: TaskQueue,
queue_not_empty: std.Thread.Condition = .{},
queue_mutex: std.Thread.Mutex = .{},
stopping: bool = false,

num_tasks: usize = 0,
next_sequence: u64 = 0,  // For absolute ordering

pub fn init(allocator: std.mem.Allocator) Self {
    return .{
        .allocator = allocator,
        .task_queue = TaskQueue.init(allocator, {}),
    };
}

pub fn deinit(self: *Self) void {
    self.stop();
    self.threads.deinit(self.allocator);
    self.task_queue.deinit();

    if (self.num_tasks > 0) {
        log.err("still have {} active tasks", .{self.num_tasks});
        std.debug.assert(self.num_tasks == 0);
    }
}

pub fn createTask(self: *Self, priority: Priority, comptime func: anytype, args: anytype) !Task {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    const task = try self.allocator.create(TaskData);
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
        .task_status = .{
            .priority = priority,
            .ctx = closure,
            .runFn = Closure.run,
            .deinitFn = Closure.deinit,
        },
    };
    task.task_status.done.set();

    self.num_tasks += 1;

    return task;
}

pub fn createRepeatingTask(self: *Self, priority: Priority, interval_ns: u64, comptime func: anytype, args: anytype) !Task {
    const task = try self.createTask(priority, func, args);
    task.task_status.interval_ns = interval_ns;
    return task;
}

fn findTaskIndex(self: *Self, target_task: Task) ?usize {
    for (self.task_queue.items, 0..) |task, index| {
        if (task == target_task) return index;
    }
    return null;
}

fn dequeue(self: *Self, task: Task) void {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    if (task.task_status.scheduled) {
        _ = self.task_queue.removeIndex(self.findTaskIndex(task) orelse return);
        task.task_status.scheduled = false;
    }

    task.task_status.reschedule = 0;
}

pub fn destroyTask(self: *Self, task: Task) void {
    self.dequeue(task);

    task.task_status.done.wait();

    task.task_status.deinitFn(task.task_status.ctx, self.allocator);
    self.allocator.destroy(task);

    std.debug.assert(self.num_tasks > 0);
    self.num_tasks -= 1;
}

pub fn scheduleTask(self: *Self, task: Task) void {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    if (task.task_status.scheduled or task.task_status.running) {
        task.task_status.reschedule += 1;
    } else {
        self.enqueue(task);
    }
}

pub fn scheduleTaskAtNs(self: *Self, task: Task, timestamp_ns: u64) void {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    if (task.task_status.running) {
        // Schedule the next run for this absolute time
        task.task_status.reschedule += 1;
        task.task_status.next_run_time_ns = timestamp_ns;
        return;
    }

    if (task.task_status.scheduled) {
        // Remove from current position using removeIndex()
        _ = self.task_queue.removeIndex(self.findTaskIndex(task) orelse return);
    }

    task.task_status.next_run_time_ns = timestamp_ns;
    self.enqueue(task);
}

pub fn scheduleTaskAfter(self: *Self, task: Task, delay_ns: u64) void {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    const current_time_ns: u64 = @intCast(std.time.nanoTimestamp());
    const run_time_ns = current_time_ns + delay_ns;

    if (task.task_status.running) {
        task.task_status.reschedule += 1;
        task.task_status.next_run_time_ns = run_time_ns;
        return;
    }

    if (task.task_status.scheduled) {
        _ = self.task_queue.removeIndex(self.findTaskIndex(task) orelse return);
    }

    task.task_status.next_run_time_ns = run_time_ns;
    self.enqueue(task);
}

pub fn cancelRepeatingTask(self: *Self, task: Task) void {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    task.task_status.interval_ns = null;
    
    // If queued and not running, unschedule immediately
    if (task.task_status.scheduled and !task.task_status.running) {
        _ = self.task_queue.removeIndex(self.findTaskIndex(task) orelse return);
        task.task_status.scheduled = false;
        task.task_status.next_run_time_ns = null;
    }
    
    // Clear any pending reschedules and complete if nothing pending
    task.task_status.reschedule = 0;
    if (!task.task_status.running) {
        task.task_status.done.set();
    }
}

fn getNextDeadline(self: *Self) ?u64 {
    return if (self.task_queue.peek()) |task| task.task_status.next_run_time_ns else null;
}

fn enqueue(self: *Self, task: *TaskData) void {
    task.task_status.scheduled = true;
    
    // Treat immediate tasks as "scheduled now" with absolute ordering
    if (task.task_status.next_run_time_ns == null) {
        const current_time: u64 = @intCast(std.time.nanoTimestamp());
        task.task_status.next_run_time_ns = current_time;
    }
    
    // Assign sequence number for absolute ordering
    task.task_status.sequence = self.next_sequence;
    self.next_sequence += 1;
    
    self.task_queue.add(task) catch |err| {
        log.err("failed to add task to queue: {}", .{err});
        // Fallback: mark as not scheduled and signal done
        task.task_status.scheduled = false;
        task.task_status.done.set();
        return;
    };
    
    self.queue_not_empty.signal();
}


fn getTaskToRun(self: *Self) ?*TaskData {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    while (!self.stopping) {
        const current_time: u64 = @intCast(std.time.nanoTimestamp());
        
        // O(1) peek at next task + O(log n) removal if ready
        if (self.task_queue.peek()) |task| {
            if (task.task_status.next_run_time_ns.? <= current_time) {
                const removed_task = self.task_queue.remove();
                removed_task.task_status.scheduled = false;
                removed_task.task_status.running = true;
                removed_task.task_status.done.reset();
                
                return removed_task;
            }
        }
        
        // Calculate timeout using next deadline (O(1) peek)
        const timeout_ns: u64 = if (self.getNextDeadline()) |deadline| blk: {
            const delta_ns: u64 = if (deadline > current_time) deadline - current_time else 0;
            break :blk delta_ns;
        } else std.time.ns_per_min;
            
        self.queue_not_empty.timedWait(&self.queue_mutex, timeout_ns) catch {};
    }
    return null;
}

fn markAsDone(self: *Self, task: *TaskData) void {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    const has_manual_reschedule = task.task_status.reschedule > 0;
    const is_repeating = task.task_status.interval_ns != null;
    
    if (has_manual_reschedule or is_repeating) {
        if (has_manual_reschedule) {
            task.task_status.reschedule -= 1;
        }
        
        if (is_repeating) {
            // If a manual absolute time was set during execution, keep it
            if (!(has_manual_reschedule and task.task_status.next_run_time_ns != null)) {
                const interval = task.task_status.interval_ns.?;
                const current_time: u64 = @intCast(std.time.nanoTimestamp());
                task.task_status.next_run_time_ns = current_time + interval;
            }
        }
        
        task.task_status.running = false;
        self.enqueue(task);
    } else {
        task.task_status.running = false;
        task.task_status.done.set();
    }
}

fn workerThreadFunc(self: *Self) void {
    while (true) {
        const task = self.getTaskToRun() orelse break;
        defer self.markAsDone(task);

        task.task_status.runFn(task.task_status.ctx);
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

    const task = try scheduler.createTask(.high, Counter.incr, .{&counter});
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

    const task = try scheduler.createRepeatingTask(.high, 100 * std.time.ns_per_ms, Counter.incr, .{&counter});
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
        start_time: u64,

        fn incr(self: *@This()) void {
            const elapsed_ns = @as(u64, @intCast(std.time.nanoTimestamp())) - self.start_time;
            if (elapsed_ns >= 200 * std.time.ns_per_ms) {
                self.count += 1;
            }
        }
    };
    var counter: Counter = .{ .start_time = @as(u64, @intCast(std.time.nanoTimestamp())) };

    const task = try scheduler.createTask(.high, Counter.incr, .{&counter});
    defer scheduler.destroyTask(task);

    scheduler.scheduleTaskAfter(task, 200 * std.time.ns_per_ms);

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

    const fast_task = try scheduler.createRepeatingTask(.high, 50 * std.time.ns_per_ms, Counter.incrFast, .{&counter});
    defer scheduler.destroyTask(fast_task);
    
    const slow_task = try scheduler.createRepeatingTask(.high, 150 * std.time.ns_per_ms, Counter.incrSlow, .{&counter});
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
