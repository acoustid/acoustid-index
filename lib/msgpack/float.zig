const std = @import("std");
const hdrs = @import("headers.zig");

const NonOptional = @import("utils.zig").NonOptional;
const maybePackNull = @import("null.zig").maybePackNull;
const maybeUnpackNull = @import("null.zig").maybeUnpackNull;

inline fn assertFloatType(comptime T: type) type {
    switch (@typeInfo(T)) {
        .Float => return T,
        .Optional => |opt_info| {
            return assertFloatType(opt_info.child);
        },
        else => @compileError("Expected float, got " ++ @typeName(T)),
    }
}

pub fn getMaxFloatSize(comptime T: type) usize {
    const Type = assertFloatType(T);
    return 1 + @sizeOf(Type);
}

pub fn getFloatSize(comptime T: type, value: T) usize {
    const Type = assertFloatType(T);
    _ = value;
    return getMaxFloatSize(Type);
}

pub fn packFloat(writer: anytype, comptime T: type, value_or_maybe_null: T) !void {
    const Type = assertFloatType(T);
    const value: Type = try maybePackNull(writer, T, value_or_maybe_null) orelse return;

    comptime var TargetType: type = undefined;
    const type_info = @typeInfo(Type);
    switch (type_info.Float.bits) {
        0...32 => {
            try writer.writeByte(hdrs.FLOAT32);
            TargetType = f32;
        },
        33...64 => {
            try writer.writeByte(hdrs.FLOAT64);
            TargetType = f64;
        },
        else => @compileError("Unsupported float size"),
    }

    const IntType = std.meta.Int(.unsigned, @bitSizeOf(TargetType));
    const int_value = @as(IntType, @bitCast(@as(TargetType, @floatCast(value))));

    var buf: [@sizeOf(IntType)]u8 = undefined;
    std.mem.writeInt(IntType, buf[0..], int_value, .big);
    try writer.writeAll(buf[0..]);
}

pub fn readFloatValue(reader: anytype, comptime SourceFloat: type, comptime TargetFloat: type) !TargetFloat {
    const size = @sizeOf(SourceFloat);
    var buf: [size]u8 = undefined;
    const actual_size = try reader.readAll(&buf);
    if (actual_size != size) {
        return error.InvalidFormat;
    }

    const IntType = std.meta.Int(.unsigned, @bitSizeOf(SourceFloat));
    const int_value = std.mem.readInt(IntType, &buf, .big);

    return @floatCast(@as(SourceFloat, @bitCast(int_value)));
}

pub fn unpackFloat(reader: anytype, comptime T: type) !T {
    const Type = assertFloatType(T);
    const header = try reader.readByte();
    switch (header) {
        hdrs.FLOAT32 => return try readFloatValue(reader, f32, Type),
        hdrs.FLOAT64 => return try readFloatValue(reader, f64, Type),
        else => return maybeUnpackNull(header, T),
    }
}

const packed_null = [_]u8{0xc0};
const packed_float32_zero = [_]u8{ 0xca, 0x00, 0x00, 0x00, 0x00 };
const packed_float64_zero = [_]u8{ 0xcb, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
const packed_float32_pi = [_]u8{ 0xca, 0x40, 0x49, 0x0f, 0xdb };
const packed_float64_pi = [_]u8{ 0xcb, 0x40, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18 };
const packed_float32_nan = [_]u8{ 0xca, 0x7f, 0xc0, 0x00, 0x00 };
const packed_float64_nan = [_]u8{ 0xcb, 0x7f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
const packed_float32_inf = [_]u8{ 0xca, 0x7f, 0x80, 0x00, 0x00 };
const packed_float64_inf = [_]u8{ 0xcb, 0x7f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };

const float_types = [_]type{ f16, f32, f64 };

fn minFloatType(comptime T1: type, comptime T2: type) type {
    const ti1 = @typeInfo(T1);
    const ti2 = @typeInfo(T2);
    return std.meta.Float(@min(ti1.Float.bits, ti2.Float.bits));
}

test "readFloat: null" {
    inline for (float_types) |T| {
        var stream = std.io.fixedBufferStream(&packed_null);
        try std.testing.expectEqual(null, try unpackFloat(stream.reader(), ?T));
    }
}

test "readFloat: float32 (zero)" {
    inline for (float_types) |T| {
        var stream = std.io.fixedBufferStream(&packed_float32_zero);
        const value = try unpackFloat(stream.reader(), T);
        try std.testing.expectApproxEqAbs(0.0, value, std.math.floatEpsAt(minFloatType(T, f32), @floatCast(value)));
    }
}

test "readFloat: float64 (zero)" {
    inline for (float_types) |T| {
        var stream = std.io.fixedBufferStream(&packed_float64_zero);
        const value = try unpackFloat(stream.reader(), T);
        try std.testing.expectApproxEqAbs(0.0, value, std.math.floatEpsAt(minFloatType(T, f64), @floatCast(value)));
    }
}

test "readFloat: float32 (pi)" {
    inline for (float_types) |T| {
        var stream = std.io.fixedBufferStream(&packed_float32_pi);
        const value = try unpackFloat(stream.reader(), T);
        try std.testing.expectApproxEqAbs(std.math.pi, value, std.math.floatEpsAt(minFloatType(T, f32), @floatCast(value)));
    }
}

test "readFloat: float64 (pi)" {
    inline for (float_types) |T| {
        var stream = std.io.fixedBufferStream(&packed_float64_pi);
        const value = try unpackFloat(stream.reader(), T);
        try std.testing.expectApproxEqAbs(std.math.pi, value, std.math.floatEpsAt(minFloatType(T, f64), @floatCast(value)));
    }
}

test "readFloat: float32 (nan)" {
    inline for (float_types) |T| {
        var stream = std.io.fixedBufferStream(&packed_float32_nan);
        const value = try unpackFloat(stream.reader(), T);
        try std.testing.expect(std.math.isNan(value));
    }
}

test "readFloat: float64 (nan)" {
    inline for (float_types) |T| {
        var stream = std.io.fixedBufferStream(&packed_float64_nan);
        const value = try unpackFloat(stream.reader(), T);
        try std.testing.expect(std.math.isNan(value));
    }
}

test "readFloat: float32 (inf)" {
    inline for (float_types) |T| {
        var stream = std.io.fixedBufferStream(&packed_float32_inf);
        const value = try unpackFloat(stream.reader(), T);
        try std.testing.expect(std.math.isInf(value));
    }
}

test "readFloat: float64 (inf)" {
    inline for (float_types) |T| {
        var stream = std.io.fixedBufferStream(&packed_float64_inf);
        const value = try unpackFloat(stream.reader(), T);
        try std.testing.expect(std.math.isInf(value));
    }
}

test "writeFloat: float32 (pi)" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packFloat(stream.writer(), f32, std.math.pi);
    try std.testing.expectEqualSlices(u8, &packed_float32_pi, stream.getWritten());
}

test "writeFloat: float64 (pi)" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packFloat(stream.writer(), f64, std.math.pi);
    try std.testing.expectEqualSlices(u8, &packed_float64_pi, stream.getWritten());
}

test "writeFloat: float32 (zero)" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packFloat(stream.writer(), f32, 0.0);
    try std.testing.expectEqualSlices(u8, &packed_float32_zero, stream.getWritten());
}

test "writeFloat: float64 (zero)" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packFloat(stream.writer(), f64, 0.0);
    try std.testing.expectEqualSlices(u8, &packed_float64_zero, stream.getWritten());
}

test "writeFloat: float32 (nan)" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packFloat(stream.writer(), f32, std.math.nan(f32));
    try std.testing.expectEqualSlices(u8, &packed_float32_nan, stream.getWritten());
}

test "writeFloat: float64 (nan)" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packFloat(stream.writer(), f64, std.math.nan(f64));
    try std.testing.expectEqualSlices(u8, &packed_float64_nan, stream.getWritten());
}

test "writeFloat: float32 (inf)" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packFloat(stream.writer(), f32, std.math.inf(f32));
    try std.testing.expectEqualSlices(u8, &packed_float32_inf, stream.getWritten());
}

test "writeFloat: float64 (inf)" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packFloat(stream.writer(), f64, std.math.inf(f64));
    try std.testing.expectEqualSlices(u8, &packed_float64_inf, stream.getWritten());
}

test "writeFloat: null" {
    inline for (float_types) |T| {
        var buffer: [16]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buffer);
        try packFloat(stream.writer(), ?T, null);
        try std.testing.expectEqualSlices(u8, &packed_null, stream.getWritten());
    }
}

test "getMaxFloatsize" {
    try std.testing.expectEqual(5, getMaxFloatSize(f32));
    try std.testing.expectEqual(9, getMaxFloatSize(f64));
}

test "getFloatSize" {
    try std.testing.expectEqual(5, getFloatSize(f32, 0.0));
    try std.testing.expectEqual(9, getFloatSize(f64, 0.0));
}
