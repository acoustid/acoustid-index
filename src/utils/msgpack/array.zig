const std = @import("std");
const c = @import("common.zig");

const NonOptional = @import("utils.zig").NonOptional;
const isOptional = @import("utils.zig").isOptional;

const maybePackNull = @import("null.zig").maybePackNull;
const maybeUnpackNull = @import("null.zig").maybeUnpackNull;

const packIntValue = @import("int.zig").packIntValue;
const unpackIntValue = @import("int.zig").unpackIntValue;

const packAny = @import("any.zig").packAny;
const unpackAny = @import("any.zig").unpackAny;

pub fn sizeOfPackedArrayHeader(len: usize) !usize {
    if (len <= c.MSG_FIXARRAY_MAX - c.MSG_FIXARRAY_MIN) {
        return 1;
    } else if (len <= std.math.maxInt(u8)) {
        return 1 + @sizeOf(u8);
    } else if (len <= std.math.maxInt(u16)) {
        return 1 + @sizeOf(u16);
    } else if (len <= std.math.maxInt(u32)) {
        return 1 + @sizeOf(u32);
    } else {
        return error.ArrayTooLong;
    }
}

pub fn sizeOfPackedArray(len: usize) !usize {
    return try sizeOfPackedArrayHeader(len) + len;
}

pub fn packArrayHeader(writer: anytype, len: usize) !void {
    if (len <= c.MSG_FIXARRAY_MAX - c.MSG_FIXARRAY_MIN) {
        try writer.writeByte(c.MSG_FIXARRAY_MIN + @as(u8, @intCast(len)));
    } else if (len <= std.math.maxInt(u16)) {
        try writer.writeByte(c.MSG_ARRAY16);
        try packIntValue(writer, u16, @intCast(len));
    } else if (len <= std.math.maxInt(u32)) {
        try writer.writeByte(c.MSG_ARRAY32);
        try packIntValue(writer, u32, @intCast(len));
    } else {
        return error.ArrayTooLong;
    }
}

pub fn unpackArrayHeader(reader: anytype, comptime T: type) !T {
    const header = try reader.readByte();
    switch (header) {
        c.MSG_FIXARRAY_MIN...c.MSG_FIXARRAY_MAX => {
            return header - c.MSG_FIXARRAY_MIN;
        },
        c.MSG_ARRAY16 => {
            return try unpackIntValue(reader, u16, NonOptional(T));
        },
        c.MSG_ARRAY32 => {
            return try unpackIntValue(reader, u32, NonOptional(T));
        },
        else => {
            return maybeUnpackNull(header, T);
        },
    }
}

pub fn packArray(writer: anytype, comptime T: type, value_or_maybe_null: T) !void {
    const value = try maybePackNull(writer, T, value_or_maybe_null) orelse return;
    try packArrayHeader(writer, value.len);

    for (value) |item| {
        try packAny(writer, T, item);
    }
}

pub fn unpackArray(reader: anytype, allocator: std.mem.Allocator, comptime T: type) !T {
    const len = if (isOptional(T))
        try unpackArrayHeader(reader, ?usize) orelse return null
    else
        try unpackArrayHeader(reader, usize);

    const Child = std.meta.Child(T);

    const data = try allocator.alloc(Child, len);
    errdefer allocator.free(data);

    for (0..len) |i| {
        data[i] = try unpackAny(reader, allocator, Child);
    }

    return data;
}

const packed_null = [_]u8{0xc0};
const packed_abc = [_]u8{ 0x93, 0x61, 0x62, 0x63 };

test "packArray: abc" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packArray(stream.writer(), []const u8, "abc");
    try std.testing.expectEqualSlices(u8, &packed_abc, stream.getWritten());
}

test "packArray: null" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packArray(stream.writer(), ?[]const u8, null);
    try std.testing.expectEqualSlices(u8, &packed_null, stream.getWritten());
}

test "sizeOfPackedArray" {
    try std.testing.expectEqual(1, sizeOfPackedArray(0));
}
