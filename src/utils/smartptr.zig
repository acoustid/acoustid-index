const std = @import("std");

const Allocator = std.mem.Allocator;

pub fn RefCounter(comptime T: type) type {
    return struct {
        refs: std.atomic.Value(T),

        pub const Self = @This();

        pub fn init() Self {
            return .{
                .refs = std.atomic.Value(u32).init(1),
            };
        }

        pub fn incr(self: *Self) void {
            const prev_ref_count = self.refs.fetchAdd(1, .monotonic);
            std.debug.assert(prev_ref_count > 0);
        }

        pub fn decr(self: *Self) bool {
            const prev_ref_count = self.refs.fetchSub(1, .monotonic);
            if (prev_ref_count == 1) {
                self.refs.fence(.acquire);
                return true;
            }
            return false;
        }
    };
}

pub fn SharedPtr(comptime T: type) type {
    return struct {
        const Inner = struct {
            refs: RefCounter(u32),
            value: T,
        };

        value: *T,

        pub const Self = @This();

        fn getInnerPtr(self: Self) *Inner {
            return @alignCast(@fieldParentPtr("value", self.value));
        }

        pub fn create(allocator: Allocator, value: T) Allocator.Error!Self {
            const inner_ptr = try allocator.create(Inner);
            inner_ptr.* = .{
                .value = value,
                .refs = RefCounter(u32).init(),
            };
            return .{ .value = &inner_ptr.value };
        }

        pub fn acquire(self: Self) Self {
            const inner_ptr = self.getInnerPtr();
            inner_ptr.refs.incr();
            return .{ .value = &inner_ptr.value };
        }

        pub fn release(self: *Self, allocator: Allocator, deinit_args: anytype) void {
            const inner_ptr = self.getInnerPtr();
            if (inner_ptr.refs.decr()) {
                if (std.meta.hasMethod(T, "deinit")) {
                    @call(.auto, T.deinit, .{&inner_ptr.value} ++ deinit_args);
                }
                allocator.destroy(inner_ptr);
                self.value = undefined;
            }
        }

        pub fn releaseWithCleanup(self: *Self, allocator: Allocator, cleanupFn: fn (*T) void, extra_args: anytype) void {
            const inner_ptr = self.getInnerPtr();
            if (inner_ptr.refs.decr()) {
                @call(.auto, cleanupFn, .{&inner_ptr.value} ++ extra_args);
                allocator.destroy(inner_ptr);
                self.value = undefined;
            }
        }

        pub fn swap(self: *Self, other: *Self) void {
            const tmp = self.value;
            self.value = other.value;
            other.value = tmp;
        }
    };
}
