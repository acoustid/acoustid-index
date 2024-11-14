const std = @import("std");
const c = @import("common.zig");

const NonOptional = @import("utils.zig").NonOptional;

const getBoolSize = @import("bool.zig").getBoolSize;
const packBool = @import("bool.zig").packBool;
const unpackBool = @import("bool.zig").unpackBool;

const getIntSize = @import("int.zig").getIntSize;
const packInt = @import("int.zig").packInt;
const unpackInt = @import("int.zig").unpackInt;

const getFloatSize = @import("float.zig").getFloatSize;
const packFloat = @import("float.zig").packFloat;
const unpackFloat = @import("float.zig").unpackFloat;

const sizeOfPackedString = @import("string.zig").sizeOfPackedString;
const packString = @import("string.zig").packString;
const unpackString = @import("string.zig").unpackString;

const sizeOfPackedArray = @import("array.zig").sizeOfPackedArray;
const packArray = @import("array.zig").packArray;
const unpackArray = @import("array.zig").unpackArray;

inline fn isString(comptime T: type) bool {
    switch (@typeInfo(T)) {
        .Pointer => |ptr_info| {
            if (ptr_info.size == .Slice) {
                if (ptr_info.child == u8) {
                    return true;
                }
            }
        },
        .Optional => |opt_info| {
            return isString(opt_info.child);
        },
        else => {},
    }
    return false;
}

pub fn sizeOfPackedAny(comptime T: type, value: T) usize {
    switch (@typeInfo(NonOptional(T))) {
        .Bool => return getBoolSize(),
        .Int => return getIntSize(T, value),
        .Float => return getFloatSize(T, value),
        .Pointer => |ptr_info| {
            if (ptr_info.size == .Slice) {
                if (isString(T)) {
                    return sizeOfPackedString(value.len);
                } else {
                    return sizeOfPackedArray(value.len);
                }
            }
        },
        else => {},
    }
    @compileError("Unsupported type '" ++ @typeName(T) ++ "'");
}

pub fn packAny(writer: anytype, comptime T: type, value: T) !void {
    switch (@typeInfo(NonOptional(T))) {
        .Bool => return packBool(writer, T, value),
        .Int => return packInt(writer, T, value),
        .Float => return packFloat(writer, T, value),
        .Pointer => |ptr_info| {
            if (ptr_info.size == .Slice) {
                if (isString(T)) {
                    return packString(writer, T, value);
                } else {
                    return packArray(writer, T, value);
                }
            }
        },
        else => {},
    }
    @compileError("Unsupported type '" + @typeName(T) + "'");
}

pub fn unpackAny(reader: anytype, allocator: std.mem.Allocator, comptime T: type) !T {
    switch (@typeInfo(NonOptional(T))) {
        .Void => return,
        .Bool => return unpackBool(reader, T),
        .Int => return unpackInt(reader, T),
        .Float => return unpackFloat(reader, T),
        .Pointer => |ptr_info| {
            if (ptr_info.size == .Slice) {
                if (isString(T)) {
                    return unpackString(reader, allocator, T);
                } else {
                    return unpackArray(reader, allocator, T);
                }
            }
        },
        else => {},
    }
    @compileError("Unsupported type '" ++ @typeName(T) ++ "'");
}

test "packAny/unpackAny: bool" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packAny(stream.writer(), bool, true);

    stream.reset();
    try std.testing.expectEqual(true, try unpackAny(stream.reader(), std.testing.allocator, bool));
}

test "packAny/unpackAny: optional bool" {
    const values = [_]?bool{ true, null };
    for (values) |value| {
        var buffer: [16]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buffer);
        try packAny(stream.writer(), ?bool, value);

        stream.reset();
        try std.testing.expectEqual(value, try unpackAny(stream.reader(), std.testing.allocator, ?bool));
    }
}

test "packAny/unpackAny: int" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packAny(stream.writer(), i32, -42);

    stream.reset();
    try std.testing.expectEqual(-42, try unpackAny(stream.reader(), std.testing.allocator, i32));
}

test "packAny/unpackAny: optional int" {
    const values = [_]?i32{ -42, null };
    for (values) |value| {
        var buffer: [16]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buffer);
        try packAny(stream.writer(), ?i32, value);

        stream.reset();
        try std.testing.expectEqual(value, try unpackAny(stream.reader(), std.testing.allocator, ?i32));
    }
}

test "packAny/unpackAny: float" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packAny(stream.writer(), f32, 3.14);

    stream.reset();
    try std.testing.expectEqual(3.14, try unpackAny(stream.reader(), std.testing.allocator, f32));
}

test "packAny/unpackAny: optional float" {
    const values = [_]?f32{ 3.14, null };
    for (values) |value| {
        var buffer: [16]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buffer);
        try packAny(stream.writer(), ?f32, value);

        stream.reset();
        try std.testing.expectEqual(value, try unpackAny(stream.reader(), std.testing.allocator, ?f32));
    }
}

test "packAny/unpackAny: string" {
    var buffer: [32]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packAny(stream.writer(), []const u8, "hello");

    stream.reset();
    const result = try unpackAny(stream.reader(), std.testing.allocator, []const u8);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("hello", result);
}

test "packAny/unpackAny: optional string" {
    const values = [_]?[]const u8{ "hello", null };
    for (values) |value| {
        var buffer: [32]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buffer);
        try packAny(stream.writer(), ?[]const u8, value);

        stream.reset();
        const result = try unpackAny(stream.reader(), std.testing.allocator, ?[]const u8);
        defer if (result) |str| std.testing.allocator.free(str);
        if (value) |str| {
            try std.testing.expectEqualStrings(str, result.?);
        } else {
            try std.testing.expectEqual(value, result);
        }
    }
}

test "packAny/unpackAny: array" {
    var buffer: [64]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    const array = [_]i32{ 1, 2, 3, 4, 5 };
    try packAny(stream.writer(), []const i32, &array);

    stream.reset();
    const result = try unpackAny(stream.reader(), std.testing.allocator, []const i32);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualSlices(i32, &array, result);
}

test "packAny/unpackAny: optional array" {
    const array = [_]i32{ 1, 2, 3, 4, 5 };
    const values = [_]?[]const i32{ &array, null };
    for (values) |value| {
        var buffer: [64]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buffer);
        try packAny(stream.writer(), ?[]const i32, value);

        stream.reset();
        const result = try unpackAny(stream.reader(), std.testing.allocator, ?[]const i32);
        defer if (result) |arr| std.testing.allocator.free(arr);
        if (value) |arr| {
            try std.testing.expectEqualSlices(i32, arr, result.?);
        } else {
            try std.testing.expectEqual(value, result);
        }
    }
}
