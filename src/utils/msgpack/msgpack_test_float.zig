const std = @import("std");
const msgpack = @import("msgpack.zig");

const float_types = [_]type{ f32, f64 };

test "readFloat: null" {
    const buffer = [_]u8{0xc0};
    inline for (float_types) |T| {
        var stream = std.io.fixedBufferStream(&buffer);
        var unpacker = msgpack.unpackerNoAlloc(stream.reader(), .{});
        try std.testing.expectEqual(null, try unpacker.read(?T));
    }
}

test "readFloat: float32 (pi)" {
    inline for (float_types) |T| {
        const buffer = [_]u8{ 0xca, 0x40, 0x49, 0x0f, 0xdb };
        var stream = std.io.fixedBufferStream(&buffer);
        var unpacker = msgpack.unpackerNoAlloc(stream.reader(), .{});
        const value = try unpacker.read(T);
        try std.testing.expectApproxEqAbs(std.math.pi, value, std.math.floatEpsAt(f32, @floatCast(value)));
    }
}

test "readFloat: float32 (zero)" {
    inline for (float_types) |T| {
        const buffer = [_]u8{ 0xca, 0x00, 0x00, 0x00, 0x00 };
        var stream = std.io.fixedBufferStream(&buffer);
        var unpacker = msgpack.unpackerNoAlloc(stream.reader(), .{});
        const value = try unpacker.read(T);
        try std.testing.expectApproxEqAbs(0.0, value, std.math.floatEpsAt(f32, @floatCast(value)));
    }
}

test "readFloat: float64 (pi)" {
    inline for (float_types) |T| {
        const buffer = [_]u8{ 0xcb, 0x40, 0x9, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18 };
        var stream = std.io.fixedBufferStream(&buffer);
        var unpacker = msgpack.unpackerNoAlloc(stream.reader(), .{});
        const value = try unpacker.read(T);
        try std.testing.expectApproxEqAbs(std.math.pi, value, std.math.floatEpsAt(if (@bitSizeOf(T) >= 64) f64 else f32, @floatCast(value)));
    }
}

test "readFloat: float64 (zero)" {
    inline for (float_types) |T| {
        const buffer = [_]u8{ 0xcb, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
        var stream = std.io.fixedBufferStream(&buffer);
        var unpacker = msgpack.unpackerNoAlloc(stream.reader(), .{});
        const value = try unpacker.read(T);
        try std.testing.expectApproxEqAbs(0.0, value, std.math.floatEpsAt(if (@bitSizeOf(T) >= 64) f64 else f32, @floatCast(value)));
    }
}

test "writeFloat: float32" {
    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    var packer = msgpack.packer(stream.writer(), .{});
    try packer.writeFloat(f32, std.math.pi);
    try std.testing.expectEqualSlices(u8, &.{ 0xca, 0x40, 0x49, 0x0f, 0xdb }, stream.getWritten());
}

test "writeFloat: float64" {
    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    var packer = msgpack.packer(stream.writer(), .{});
    try packer.writeFloat(f64, std.math.pi);
    try std.testing.expectEqualSlices(u8, &.{ 0xcb, 0x40, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18 }, stream.getWritten());
}
