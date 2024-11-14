const std = @import("std");
const c = @import("common.zig");

const getBoolSize = @import("bool.zig").getBoolSize;
const packBool = @import("bool.zig").packBool;
const unpackBool = @import("bool.zig").unpackBool;

const getIntSize = @import("int.zig").getIntSize;
const packInt = @import("int.zig").packInt;
const unpackInt = @import("int.zig").unpackInt;

const getFloatSize = @import("float.zig").getFloatSize;
const packFloat = @import("float.zig").packFloat;
const unpackFloat = @import("float.zig").unpackFloat;

pub fn sizeOfPackedAny(comptime T: type, value: T) usize {
    const type_info = @typeInfo(T);
    switch (type_info) {
        .Bool => return getBoolSize(),
        .Int => return getIntSize(T, value),
        .Float => return getFloatSize(T, value),
        .Optional => {
            const child_type_info = @typeInfo(type_info.Optional.child);
            switch (child_type_info) {
                .Bool => return getBoolSize(),
                .Int => return getIntSize(T, value),
                .Float => return getFloatSize(T, value),
                else => {},
            }
        },
        else => {},
    }
    @compileError("Unsupported type '" ++ @typeName(T) ++ "'");
}

pub fn packAny(writer: anytype, comptime T: type, value: T) !void {
    const type_info = @typeInfo(T);
    switch (type_info) {
        .Bool => return packBool(writer, T, value),
        .Int => return packInt(writer, T, value),
        .Float => return packFloat(writer, T, value),
        .Optional => {
            const child_type_info = @typeInfo(type_info.Optional.child);
            switch (child_type_info) {
                .Bool => return packBool(writer, T, value),
                .Int => return packInt(writer, T, value),
                .Float => return packFloat(writer, T, value),
                else => {},
            }
        },
        else => {},
    }
    @compileError("Unsupported type '" + @typeName(T) + "'");
}

pub fn unpackAny(reader: anytype, comptime T: type) !T {
    const type_info = @typeInfo(T);
    switch (type_info) {
        .Void => return,
        .Bool => return unpackBool(reader, T),
        .Int => return unpackInt(reader, T),
        .Float => return unpackFloat(reader, T),
        .Optional => {
            const child_type_info = @typeInfo(type_info.Optional.child);
            switch (child_type_info) {
                .Void => return,
                .Bool => return unpackBool(reader, T),
                .Int => return unpackInt(reader, T),
                .Float => return unpackFloat(reader, T),
                else => {},
            }
        },
        else => {},
    }
    @compileError("Unsupported type '" + @typeName(T) + "'");
}

test "packAny/unpackAny: bool" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packAny(stream.writer(), bool, true);

    stream.reset();
    try std.testing.expectEqual(true, try unpackAny(stream.reader(), bool));
}

test "packAny/unpackAny: optional bool" {
    const values = [_]?bool{ true, null };
    for (values) |value| {
        var buffer: [16]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buffer);
        try packAny(stream.writer(), ?bool, value);

        stream.reset();
        try std.testing.expectEqual(value, try unpackAny(stream.reader(), ?bool));
    }
}

test "packAny/unpackAny: int" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packAny(stream.writer(), i32, -42);

    stream.reset();
    try std.testing.expectEqual(-42, try unpackAny(stream.reader(), i32));
}

test "packAny/unpackAny: optional int" {
    const values = [_]?i32{ -42, null };
    for (values) |value| {
        var buffer: [16]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buffer);
        try packAny(stream.writer(), ?i32, value);

        stream.reset();
        try std.testing.expectEqual(value, try unpackAny(stream.reader(), ?i32));
    }
}

test "packAny/unpackAny: float" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packAny(stream.writer(), f32, 3.14);

    stream.reset();
    try std.testing.expectEqual(3.14, try unpackAny(stream.reader(), f32));
}

test "packAny/unpackAny: optional float" {
    const values = [_]?f32{ 3.14, null };
    for (values) |value| {
        var buffer: [16]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buffer);
        try packAny(stream.writer(), ?f32, value);

        stream.reset();
        try std.testing.expectEqual(value, try unpackAny(stream.reader(), ?f32));
    }
}
