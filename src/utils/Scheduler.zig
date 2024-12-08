const std = @import("std");
const log = std.log.scoped(.scheduler);

const Priority = enum(u8) {
    high = 0,
    medium = 1,
    low = 2,
    do_not_run = 3,
};

pub fn Future(comptime T: type) type {
    const Started = enum(u8) {
        not_started,
        started,
        cancelled,
    };

    return struct {
        result: ?T = null,
        cancelled: std.atomic.Value(bool),
        started: std.atomic.Value(Started),
        finished: std.Thread.ResetEvent,

        pub fn init() @This() {
            return .{
                .cancelled = std.atomic.Value(bool).init(false),
                .started = std.atomic.Value(Started).init(.not_started),
                .finished = .{},
            };
        }

        pub fn cancel(self: *@This()) void {
            self.markAsCancelled() catch return;
            self.markAsStarted(.cancelled) catch return;
            self.markAsFinished(null);
        }

        pub fn isCancelled(self: @This()) bool {
            return self.cancelled.load(.acquire);
        }

        pub fn isStarted(self: @This()) bool {
            return self.started.load(.acquire) == .started;
        }

        pub fn isRunning(self: @This()) bool {
            return self.isStarted() and !self.isFinished();
        }

        pub fn isFinished(self: @This()) bool {
            return self.finished.isSet();
        }

        pub fn waitForFinished(self: *@This()) void {
            self.finished.wait();
        }

        pub fn getResult(self: @This()) ?T {
            if (self.isFinished()) {
                return self.result;
            }
            return null;
        }

        fn markAsCancelled(self: *@This()) !void {
            const res = self.cancelled.cmpxchgStrong(false, true, .seq_cst, .seq_cst);
            if (res != null) {
                return error.AlreadyCancelled;
            }
        }

        fn markAsStarted(self: *@This(), status: Started) !void {
            std.debug.assert(status != .not_started);
            const res = self.started.cmpxchgStrong(.not_started, status, .seq_cst, .seq_cst);
            if (res != null) {
                return error.AlreadyStarted;
            }
        }

        fn markAsFinished(self: *@This(), result: ?T) void {
            std.debug.assert(!self.finished.isSet());
            self.result = result;
            self.finished.set();
        }
    };
}

test "Future" {
    var fut = Future(u8).init();
    try std.testing.expectEqual(false, fut.isStarted());
    try std.testing.expectEqual(false, fut.isRunning());
    try std.testing.expectEqual(false, fut.isFinished());
    try std.testing.expectEqual(false, fut.isCancelled());
    try std.testing.expectEqual(null, fut.getResult());

    fut.cancel();
    fut.waitForFinished();

    try std.testing.expectEqual(false, fut.isStarted());
    try std.testing.expectEqual(false, fut.isRunning());
    try std.testing.expectEqual(true, fut.isFinished());
    try std.testing.expectEqual(true, fut.isCancelled());
    try std.testing.expectEqual(null, fut.getResult());
}

const TaskStatus = struct {
    reschedule: usize = 0,
    scheduled: bool = false,
    running: bool = false,
    done: std.Thread.ResetEvent = .{},
    priority: Priority,
    ctx: *anyopaque,
    runFn: *const fn (ctx: *anyopaque) void,
    deinitFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator) void,
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

num_tasks: usize = 0,

pub fn init(allocator: std.mem.Allocator) Self {
    return .{
        .allocator = allocator,
    };
}

pub fn deinit(self: *Self) void {
    self.stop();
    self.threads.deinit(self.allocator);

    std.debug.assert(self.num_tasks == 0);
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

fn dequeue(self: *Self, task: Task) void {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    if (task.data.scheduled) {
        self.queue.remove(task);
        task.next = null;
        task.prev = null;
        task.data.scheduled = false;
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

fn enqueue(self: *Self, task: *Queue.Node) void {
    task.data.scheduled = true;
    self.queue.prepend(task);
    self.queue_not_empty.signal();
}

fn getTaskToRun(self: *Self) ?*Queue.Node {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    while (!self.stopping) {
        const task = self.queue.popFirst() orelse {
            self.queue_not_empty.timedWait(&self.queue_mutex, std.time.us_per_min) catch {};
            continue;
        };
        task.prev = null;
        task.next = null;
        task.data.scheduled = false;
        task.data.running = true;
        task.data.done.reset();
        return task;
    }
    return null;
}

fn markAsDone(self: *Self, task: *Queue.Node) void {
    self.queue_mutex.lock();
    defer self.queue_mutex.unlock();

    if (task.data.reschedule > 0) {
        task.data.reschedule -= 1;
        self.enqueue(task);
    }

    task.data.running = false;
    task.data.done.set();
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
