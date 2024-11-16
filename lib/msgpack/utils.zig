const std = @import("std");

pub fn NonOptional(comptime T: type) type {
    const type_info = @typeInfo(T);
    if (type_info == .Optional) {
        return type_info.Optional.child;
    }
    return T;
}

pub fn Optional(comptime T: type, comptime is_optional: bool) type {
    return if (is_optional) ?T else T;
}

pub inline fn isOptional(comptime T: type) bool {
    return @typeInfo(T) == .Optional;
}

test isOptional {
    try std.testing.expect(isOptional(?u32));
    try std.testing.expect(!isOptional(u32));
}

var no_allocator_dummy: u8 = 0;

pub const NoAllocator = struct {
    pub fn noAlloc(ctx: *anyopaque, len: usize, ptr_align: u8, ret_addr: usize) ?[*]u8 {
        _ = ctx;
        _ = len;
        _ = ptr_align;
        _ = ret_addr;
        return null;
    }

    pub fn allocator() std.mem.Allocator {
        return .{
            .ptr = &no_allocator_dummy,
            .vtable = &.{
                .alloc = noAlloc,
                .resize = std.mem.Allocator.noResize,
                .free = std.mem.Allocator.noFree,
            },
        };
    }
};
