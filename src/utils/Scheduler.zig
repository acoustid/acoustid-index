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
    next_run_time: ?i64 = null,
};

const Queue = std.DoublyLinkedList(TaskStatus);
pub const Task = *Queue.Node;

const Self = @This();

allocator: std.mem.Allocator,
threads: std.ArrayListUnmanaged(std.Thread) = .{},

queue: Queue = .{},
queue_not_empty: std.Thread.Condition = .{},
queue_mutex: std.Thread.Mutex = .{},
stopping: bool = false,
next_deadline: ?i64 = null,

num_tasks: usize = 0,

pub fn init(allocator: std.mem.Allocator) Self {
    return .{
        .allocator = allocator,
    };
}

pub fn deinit(self: *Self) void {
    self.stop();
    self.threads.deinit(self.allocator);

    if (self.num_tasks > 0) {
        log.err("still have {} active tasks", .{self.num_tasks});
        std.debug.assert(self.num_tasks == 0);
    }
}

pub fn createTask(self: *Self, priority: Priority, comptime func: anytype, args: anytype) !Task {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    const task = try self.allocator.create(Queue.Node);
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
        .data = .{
            .priority = priority,
            .ctx = closure,
            .runFn = Closure.run,
            .deinitFn = Closure.deinit,
        },
    };
    task.data.done.set();

    self.num_tasks += 1;

    return task;
}

pub fn createRepeatingTask(self: *Self, priority: Priority, interval_ns: u64, comptime func: anytype, args: anytype) !Task {
    const task = try self.createTask(priority, func, args);
    task.data.interval_ns = interval_ns;
    return task;
}

fn dequeue(self: *Self, task: Task) void {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    if (task.data.scheduled) {
        const was_next_deadline = task.data.next_run_time != null and 
            self.next_deadline != null and task.data.next_run_time.? == self.next_deadline.?;
        
        self.queue.remove(task);
        task.next = null;
        task.prev = null;
        task.data.scheduled = false;
        
        if (was_next_deadline) {
            self.updateNextDeadline();
        }
    }

    task.data.reschedule = 0;
}

pub fn destroyTask(self: *Self, task: Task) void {
    self.dequeue(task);

    task.data.done.wait();

    task.data.deinitFn(task.data.ctx, self.allocator);
    self.allocator.destroy(task);

    std.debug.assert(self.num_tasks > 0);
    self.num_tasks -= 1;
}

pub fn scheduleTask(self: *Self, task: Task) void {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    if (task.data.scheduled or task.data.running) {
        task.data.reschedule += 1;
    } else {
        self.enqueue(task);
    }
}

pub fn scheduleTaskAt(self: *Self, task: Task, timestamp_ms: i64) void {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    if (task.data.scheduled or task.data.running) {
        task.data.reschedule += 1;
    } else {
        task.data.next_run_time = timestamp_ms;
        self.enqueue(task);
    }
}

pub fn scheduleTaskAfter(self: *Self, task: Task, delay_ns: u64) void {
    const delay_ms = delay_ns / std.time.ns_per_ms;
    const run_time = std.time.milliTimestamp() + @as(i64, @intCast(delay_ms));
    self.scheduleTaskAt(task, run_time);
}

pub fn cancelRepeatingTask(self: *Self, task: Task) void {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    task.data.interval_ns = null;
    
    if (!task.data.running and task.data.reschedule == 0) {
        task.data.done.set();
    }
}

fn updateNextDeadline(self: *Self) void {
    self.next_deadline = null;
    var it = self.queue.first;
    while (it) |node| : (it = node.next) {
        if (node.data.next_run_time) |run_time| {
            if (self.next_deadline == null or run_time < self.next_deadline.?) {
                self.next_deadline = run_time;
            }
        }
    }
}

fn enqueue(self: *Self, task: *Queue.Node) void {
    task.data.scheduled = true;
    
    if (task.data.next_run_time == null) {
        self.queue.prepend(task);
    } else {
        const run_time = task.data.next_run_time.?;
        var current = self.queue.first;
        
        while (current) |node| {
            if (node.data.next_run_time == null or run_time <= node.data.next_run_time.?) {
                self.queue.insertBefore(node, task);
                break;
            }
            current = node.next;
        } else {
            self.queue.append(task);
        }
        
        if (self.next_deadline == null or run_time < self.next_deadline.?) {
            self.next_deadline = run_time;
        }
    }
    
    self.queue_not_empty.signal();
}

fn getTaskToRun(self: *Self) ?*Queue.Node {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    while (!self.stopping) {
        const current_time = std.time.milliTimestamp();
        
        var current = self.queue.first;
        while (current) |task| {
            const next_task = task.next;
            
            if (task.data.next_run_time == null or task.data.next_run_time.? <= current_time) {
                self.queue.remove(task);
                task.prev = null;
                task.next = null;
                task.data.scheduled = false;
                task.data.running = true;
                task.data.done.reset();
                
                if (task.data.next_run_time != null and self.next_deadline != null and 
                    task.data.next_run_time.? == self.next_deadline.?) {
                    self.updateNextDeadline();
                }
                
                return task;
            }
            
            current = next_task;
        }
        
        const timeout_ns = if (self.next_deadline) |deadline| 
            @max(0, (deadline - current_time) * std.time.ns_per_ms)
        else 
            std.time.ns_per_min;
            
        self.queue_not_empty.timedWait(&self.queue_mutex, timeout_ns) catch {};
    }
    return null;
}

fn markAsDone(self: *Self, task: *Queue.Node) void {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    const has_manual_reschedule = task.data.reschedule > 0;
    const is_repeating = task.data.interval_ns != null;
    
    if (has_manual_reschedule or is_repeating) {
        if (has_manual_reschedule) {
            task.data.reschedule -= 1;
        }
        
        if (is_repeating) {
            const interval = task.data.interval_ns.?;
            task.data.next_run_time = std.time.milliTimestamp() + @as(i64, @intCast(interval / std.time.ns_per_ms));
        }
        
        task.data.running = false;
        self.enqueue(task);
    } else {
        task.data.running = false;
        task.data.done.set();
    }
}

fn workerThreadFunc(self: *Self) void {
    while (true) {
        const task = self.getTaskToRun() orelse break;
        defer self.markAsDone(task);

        task.data.runFn(task.data.ctx);
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
        start_time: i64,

        fn incr(self: *@This()) void {
            const elapsed = std.time.milliTimestamp() - self.start_time;
            if (elapsed >= 200) {
                self.count += 1;
            }
        }
    };
    var counter: Counter = .{ .start_time = std.time.milliTimestamp() };

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
