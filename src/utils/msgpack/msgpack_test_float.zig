const std = @import("std");
const msgpack = @import("msgpack.zig");

const packed_null = [_]u8{0xc0};
const packed_float32_zero = [_]u8{ 0xca, 0x00, 0x00, 0x00, 0x00 };
const packed_float64_zero = [_]u8{ 0xcb, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
const packed_float32_pi = [_]u8{ 0xca, 0x40, 0x49, 0x0f, 0xdb };
const packed_float64_pi = [_]u8{ 0xcb, 0x40, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18 };
const packed_float32_nan = [_]u8{ 0xca, 0x7f, 0xc0, 0x00, 0x00 };
const packed_float64_nan = [_]u8{ 0xcb, 0x7f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
const packed_float32_inf = [_]u8{ 0xca, 0x7f, 0x80, 0x00, 0x00 };
const packed_float64_inf = [_]u8{ 0xcb, 0x7f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };

const float_types = [_]type{ f32, f64 };

test "readFloat: null" {
    inline for (float_types) |T| {
        var stream = std.io.fixedBufferStream(&packed_null);
        try std.testing.expectEqual(null, try msgpack.unpack(?T, stream.reader(), .{}));
    }
}

test "readFloat: float32 (zero)" {
    inline for (float_types) |T| {
        var stream = std.io.fixedBufferStream(&packed_float32_zero);
        const value = try msgpack.unpack(T, stream.reader(), .{});
        try std.testing.expectApproxEqAbs(0.0, value, std.math.floatEpsAt(f32, @floatCast(value)));
    }
}

test "readFloat: float64 (zero)" {
    inline for (float_types) |T| {
        var stream = std.io.fixedBufferStream(&packed_float64_zero);
        const value = try msgpack.unpack(T, stream.reader(), .{});
        try std.testing.expectApproxEqAbs(0.0, value, std.math.floatEpsAt(T, @floatCast(value)));
    }
}

test "readFloat: float32 (pi)" {
    inline for (float_types) |T| {
        var stream = std.io.fixedBufferStream(&packed_float32_pi);
        const value = try msgpack.unpack(T, stream.reader(), .{});
        try std.testing.expectApproxEqAbs(std.math.pi, value, std.math.floatEpsAt(f32, @floatCast(value)));
    }
}

test "readFloat: float64 (pi)" {
    inline for (float_types) |T| {
        var stream = std.io.fixedBufferStream(&packed_float64_pi);
        const value = try msgpack.unpack(T, stream.reader(), .{});
        try std.testing.expectApproxEqAbs(std.math.pi, value, std.math.floatEpsAt(T, @floatCast(value)));
    }
}

test "readFloat: float32 (nan)" {
    inline for (float_types) |T| {
        var stream = std.io.fixedBufferStream(&packed_float32_nan);
        const value = try msgpack.unpack(T, stream.reader(), .{});
        try std.testing.expect(std.math.isNan(value));
    }
}

test "readFloat: float64 (nan)" {
    inline for (float_types) |T| {
        var stream = std.io.fixedBufferStream(&packed_float64_nan);
        const value = try msgpack.unpack(T, stream.reader(), .{});
        try std.testing.expect(std.math.isNan(value));
    }
}

test "readFloat: float32 (inf)" {
    inline for (float_types) |T| {
        var stream = std.io.fixedBufferStream(&packed_float32_inf);
        const value = try msgpack.unpack(T, stream.reader(), .{});
        try std.testing.expect(std.math.isInf(value));
    }
}

test "readFloat: float64 (inf)" {
    inline for (float_types) |T| {
        var stream = std.io.fixedBufferStream(&packed_float64_inf);
        const value = try msgpack.unpack(T, stream.reader(), .{});
        try std.testing.expect(std.math.isInf(value));
    }
}

test "writeFloat: float32 (pi)" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try msgpack.pack(f32, stream.writer(), std.math.pi);
    try std.testing.expectEqualSlices(u8, &packed_float32_pi, stream.getWritten());
}

test "writeFloat: float64 (pi)" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try msgpack.pack(f64, stream.writer(), std.math.pi);
    try std.testing.expectEqualSlices(u8, &packed_float64_pi, stream.getWritten());
}

test "writeFloat: float32 (zero)" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try msgpack.pack(f32, stream.writer(), 0.0);
    try std.testing.expectEqualSlices(u8, &packed_float32_zero, stream.getWritten());
}

test "writeFloat: float64 (zero)" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try msgpack.pack(f64, stream.writer(), 0.0);
    try std.testing.expectEqualSlices(u8, &packed_float64_zero, stream.getWritten());
}

test "writeFloat: float32 (nan)" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try msgpack.pack(f32, stream.writer(), std.math.nan(f32));
    try std.testing.expectEqualSlices(u8, &packed_float32_nan, stream.getWritten());
}

test "writeFloat: float64 (nan)" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try msgpack.pack(f64, stream.writer(), std.math.nan(f64));
    try std.testing.expectEqualSlices(u8, &packed_float64_nan, stream.getWritten());
}

test "writeFloat: float32 (inf)" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try msgpack.pack(f32, stream.writer(), std.math.inf(f32));
    try std.testing.expectEqualSlices(u8, &packed_float32_inf, stream.getWritten());
}

test "writeFloat: float64 (inf)" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try msgpack.pack(f64, stream.writer(), std.math.inf(f64));
    try std.testing.expectEqualSlices(u8, &packed_float64_inf, stream.getWritten());
}

test "writeFloat: null" {
    inline for (float_types) |T| {
        var buffer: [16]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buffer);
        try msgpack.pack(?T, stream.writer(), null);
        try std.testing.expectEqualSlices(u8, &packed_null, stream.getWritten());
    }
}
