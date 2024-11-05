const std = @import("std");

const Job = struct {
    ctx: *anyopaque,
    func: *const fn (*anyopaque) void,

    pub fn init(comptime Context: type, func: fn (*Context) void, ctx: *Context) Job {
        const wrapper = struct {
            pub fn innerFunc(ptr: *anyopaque) void {
                const typed_ctx: *Context = @alignCast(@ptrCast(ptr));
                @call(.always_inline, func, .{typed_ctx});
            }
        };

        return .{
            .ctx = ctx,
            .func = wrapper.innerFunc,
        };
    }

    pub fn run(self: Job) void {
        self.func(self.ctx);
    }
};

const JobQueue = std.SinglyLinkedList(Job);

const Self = @This();

allocator: std.mem.Allocator,
queue: JobQueue = .{},
thread: ?std.Thread = null,

pub fn init(allocator: std.mem.Allocator) Self {
    return .{
        .allocator = allocator,
    };
}

pub fn deinit(self: *Self) void {
    while (self.queue.popFirst()) |node| {
        self.allocator.destroy(node);
    }
}

pub fn schedule(self: *Self, comptime Context: type, task: anytype, ctx: Context) void {
    var node = try std.allocator.create(JobQueue.Node);
    node.data = Job.init(Context, task, ctx);
    self.queue.prepend(node);
}

test "job" {
    const Task = struct {
        value: usize = 0,

        pub fn incr(self: *@This()) void {
            self.value += 1;
        }
    };

    var task: Task = .{};

    const job = Job.init(Task, Task.incr, &task);
    job.run();

    try std.testing.expect(task.value == 1);
}
