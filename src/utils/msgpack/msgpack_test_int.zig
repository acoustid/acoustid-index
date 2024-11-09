const std = @import("std");
const msgpack = @import("msgpack.zig");

const int_types = [_]type{ i8, i16, i32, i64, u8, u16, u32, u64 };

test "readInt: positive fixint" {
    const buffer = [_]u8{0x7f};
    inline for (int_types) |T| {
        var stream = std.io.fixedBufferStream(&buffer);
        var unpacker = msgpack.unpacker(stream.reader(), .{});
        try std.testing.expectEqual(127, try unpacker.readInt(T));
    }
}

test "readInt: negative fixint" {
    const buffer = [_]u8{0xe0};
    inline for (int_types) |T| {
        var stream = std.io.fixedBufferStream(&buffer);
        var unpacker = msgpack.unpacker(stream.reader(), .{});
        const info = @typeInfo(T).Int;
        if (info.signedness == .unsigned) {
            try std.testing.expectError(error.IntegerOverflow, unpacker.readInt(T));
        } else {
            try std.testing.expectEqual(-32, try unpacker.readInt(T));
        }
    }
}

test "readInt: uint8" {
    const buffer = [_]u8{ 0xcc, 0xff };
    inline for (int_types) |T| {
        var stream = std.io.fixedBufferStream(&buffer);
        var unpacker = msgpack.unpacker(stream.reader(), .{});
        const info = @typeInfo(T).Int;
        if (info.bits < 8 or (info.bits == 8 and info.signedness == .signed)) {
            try std.testing.expectError(error.IntegerOverflow, unpacker.readInt(T));
        } else {
            try std.testing.expectEqual(0xff, try unpacker.readInt(T));
        }
    }
}

test "readInt: uint16" {
    const buffer = [_]u8{ 0xcd, 0xff, 0xff };
    inline for (int_types) |T| {
        var stream = std.io.fixedBufferStream(&buffer);
        var unpacker = msgpack.unpacker(stream.reader(), .{});
        const info = @typeInfo(T).Int;
        if (info.bits < 16 or (info.bits == 16 and info.signedness == .signed)) {
            try std.testing.expectError(error.IntegerOverflow, unpacker.readInt(T));
        } else {
            try std.testing.expectEqual(0xffff, try unpacker.readInt(T));
        }
    }
}

test "readInt: uint32" {
    const buffer = [_]u8{ 0xce, 0xff, 0xff, 0xff, 0xff };
    inline for (int_types) |T| {
        var stream = std.io.fixedBufferStream(&buffer);
        var unpacker = msgpack.unpacker(stream.reader(), .{});
        const info = @typeInfo(T).Int;
        if (info.bits < 32 or (info.bits == 32 and info.signedness == .signed)) {
            try std.testing.expectError(error.IntegerOverflow, unpacker.readInt(T));
        } else {
            try std.testing.expectEqual(0xffffffff, try unpacker.readInt(T));
        }
    }
}

test "readInt: uint64" {
    const buffer = [_]u8{ 0xcf, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff };
    inline for (int_types) |T| {
        var stream = std.io.fixedBufferStream(&buffer);
        var unpacker = msgpack.unpacker(stream.reader(), .{});
        const info = @typeInfo(T).Int;
        if (info.bits < 64 or (info.bits == 64 and info.signedness == .signed)) {
            try std.testing.expectError(error.IntegerOverflow, unpacker.readInt(T));
        } else {
            try std.testing.expectEqual(0xffffffffffffffff, try unpacker.readInt(T));
        }
    }
}

test "readInt: negative int8" {
    const buffer = [_]u8{ 0xd0, 0x80 };
    inline for (int_types) |T| {
        var stream = std.io.fixedBufferStream(&buffer);
        var unpacker = msgpack.unpacker(stream.reader(), .{});
        const info = @typeInfo(T).Int;
        if (info.signedness == .unsigned or info.bits < 8) {
            try std.testing.expectError(error.IntegerOverflow, unpacker.readInt(T));
        } else {
            try std.testing.expectEqual(-128, try unpacker.readInt(T));
        }
    }
}

test "readInt: positive int8" {
    const buffer = [_]u8{ 0xd0, 0x7f };
    inline for (int_types) |T| {
        var stream = std.io.fixedBufferStream(&buffer);
        var unpacker = msgpack.unpacker(stream.reader(), .{});
        const info = @typeInfo(T).Int;
        if (info.bits < 7) {
            try std.testing.expectError(error.IntegerOverflow, unpacker.readInt(T));
        } else {
            try std.testing.expectEqual(127, try unpacker.readInt(T));
        }
    }
}

test "readInt: negative int16" {
    const buffer = [_]u8{ 0xd1, 0x80, 0x00 };
    inline for (int_types) |T| {
        var stream = std.io.fixedBufferStream(&buffer);
        var unpacker = msgpack.unpacker(stream.reader(), .{});
        const info = @typeInfo(T).Int;
        if (info.signedness == .unsigned or info.bits < 16) {
            try std.testing.expectError(error.IntegerOverflow, unpacker.readInt(T));
        } else {
            try std.testing.expectEqual(-32768, try unpacker.readInt(T));
        }
    }
}

test "readInt: positive int16" {
    const buffer = [_]u8{ 0xd1, 0x7f, 0xff };
    inline for (int_types) |T| {
        var stream = std.io.fixedBufferStream(&buffer);
        var unpacker = msgpack.unpacker(stream.reader(), .{});
        const info = @typeInfo(T).Int;
        if (info.bits < 15) {
            try std.testing.expectError(error.IntegerOverflow, unpacker.readInt(T));
        } else {
            try std.testing.expectEqual(32767, try unpacker.readInt(T));
        }
    }
}

test "readInt: negative int32" {
    const buffer = [_]u8{ 0xd2, 0x80, 0x00, 0x00, 0x00 };
    inline for (int_types) |T| {
        var stream = std.io.fixedBufferStream(&buffer);
        var unpacker = msgpack.unpacker(stream.reader(), .{});
        const info = @typeInfo(T).Int;
        if (info.signedness == .unsigned or info.bits < 32) {
            try std.testing.expectError(error.IntegerOverflow, unpacker.readInt(T));
        } else {
            try std.testing.expectEqual(-2147483648, try unpacker.readInt(T));
        }
    }
}

test "readInt: positive int32" {
    const buffer = [_]u8{ 0xd2, 0x7f, 0xff, 0xff, 0xff };
    inline for (int_types) |T| {
        var stream = std.io.fixedBufferStream(&buffer);
        var unpacker = msgpack.unpacker(stream.reader(), .{});
        const info = @typeInfo(T).Int;
        if (info.bits < 31) {
            try std.testing.expectError(error.IntegerOverflow, unpacker.readInt(T));
        } else {
            try std.testing.expectEqual(2147483647, try unpacker.readInt(T));
        }
    }
}

test "readInt: negative int64" {
    const buffer = [_]u8{ 0xd3, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
    inline for (int_types) |T| {
        var stream = std.io.fixedBufferStream(&buffer);
        var unpacker = msgpack.unpacker(stream.reader(), .{});
        const info = @typeInfo(T).Int;
        if (info.signedness == .unsigned or info.bits < 64) {
            try std.testing.expectError(error.IntegerOverflow, unpacker.readInt(T));
        } else {
            try std.testing.expectEqual(-9223372036854775808, try unpacker.readInt(T));
        }
    }
}

test "readInt: positive int64" {
    const buffer = [_]u8{ 0xd3, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff };
    inline for (int_types) |T| {
        var stream = std.io.fixedBufferStream(&buffer);
        var unpacker = msgpack.unpacker(stream.reader(), .{});
        const info = @typeInfo(T).Int;
        if (info.bits < 63) {
            try std.testing.expectError(error.IntegerOverflow, unpacker.readInt(T));
        } else {
            try std.testing.expectEqual(9223372036854775807, try unpacker.readInt(T));
        }
    }
}

test "writeInt: positive fixint" {
    var buffer: [100]u8 = undefined;
    inline for (int_types) |T| {
        var stream = std.io.fixedBufferStream(&buffer);
        var packer = msgpack.packer(stream.writer(), .{});
        try packer.writeInt(T, 127);
        try std.testing.expectEqualSlices(u8, &.{0x7f}, stream.getWritten());
    }
}

test "writeInt: negative fixint" {
    var buffer: [100]u8 = undefined;
    inline for (int_types) |T| {
        const info = @typeInfo(T).Int;
        if (info.signedness == .signed) {
            var stream = std.io.fixedBufferStream(&buffer);
            var packer = msgpack.packer(stream.writer(), .{});
            try packer.writeInt(i8, -32);
            try std.testing.expectEqualSlices(u8, &.{0xE0}, stream.getWritten());
        }
    }
}

test "writeInt: uint8" {
    var buffer: [100]u8 = undefined;
    inline for (int_types) |T| {
        const info = @typeInfo(T).Int;
        if (info.signedness == .unsigned and info.bits >= 8) {
            var stream = std.io.fixedBufferStream(&buffer);
            var packer = msgpack.packer(stream.writer(), .{});
            try packer.writeInt(T, 200);
            try std.testing.expectEqualSlices(u8, &.{ 0xcc, 200 }, stream.getWritten());
        }
    }
}

test "writeInt: uint16" {
    var buffer: [100]u8 = undefined;
    inline for (int_types) |T| {
        const info = @typeInfo(T).Int;
        if (info.signedness == .unsigned and info.bits >= 16) {
            var stream = std.io.fixedBufferStream(&buffer);
            var packer = msgpack.packer(stream.writer(), .{});
            try packer.writeInt(T, 40000);
            try std.testing.expectEqualSlices(u8, &.{ 0xcd, 0x9c, 0x40 }, stream.getWritten());
        }
    }
}

test "writeInt: uint32" {
    var buffer: [100]u8 = undefined;
    inline for (int_types) |T| {
        const info = @typeInfo(T).Int;
        if (info.signedness == .unsigned and info.bits >= 32) {
            var stream = std.io.fixedBufferStream(&buffer);
            var packer = msgpack.packer(stream.writer(), .{});
            try packer.writeInt(T, 3000000000);
            try std.testing.expectEqualSlices(u8, &.{ 0xce, 0xb2, 0xd0, 0x5e, 0x00 }, stream.getWritten());
        }
    }
}

test "writeInt: uint64" {
    var buffer: [100]u8 = undefined;
    inline for (int_types) |T| {
        const info = @typeInfo(T).Int;
        if (info.signedness == .unsigned and info.bits >= 64) {
            var stream = std.io.fixedBufferStream(&buffer);
            var packer = msgpack.packer(stream.writer(), .{});
            try packer.writeInt(T, 9000000000000000000);
            try std.testing.expectEqualSlices(u8, &.{ 0xcf, 0x7c, 0xe6, 0x6c, 0x50, 0xe2, 0x84, 0x0, 0x0 }, stream.getWritten());
        }
    }
}

test "writeInt: positive int8" {
    var buffer: [100]u8 = undefined;
    inline for (int_types) |T| {
        const info = @typeInfo(T).Int;
        if (info.signedness == .signed and info.bits > 8) {
            var stream = std.io.fixedBufferStream(&buffer);
            var packer = msgpack.packer(stream.writer(), .{});
            try packer.writeInt(T, 100);
            try std.testing.expectEqualSlices(u8, &.{100}, stream.getWritten());
        }
    }
}

test "writeInt: negative int8" {
    var buffer: [100]u8 = undefined;
    inline for (int_types) |T| {
        const info = @typeInfo(T).Int;
        if (info.signedness == .signed and info.bits >= 8) {
            var stream = std.io.fixedBufferStream(&buffer);
            var packer = msgpack.packer(stream.writer(), .{});
            try packer.writeInt(T, -100);
            try std.testing.expectEqualSlices(u8, &.{ 0xd0, 156 }, stream.getWritten());
        }
    }
}

test "writeInt: positive int16" {
    var buffer: [100]u8 = undefined;
    inline for (int_types) |T| {
        const info = @typeInfo(T).Int;
        if (info.signedness == .signed and info.bits >= 16) {
            var stream = std.io.fixedBufferStream(&buffer);
            var packer = msgpack.packer(stream.writer(), .{});
            try packer.writeInt(T, 20000);
            try std.testing.expectEqualSlices(u8, &.{ 0xd1, 0x4e, 0x20 }, stream.getWritten());
        }
    }
}

test "writeInt: negative int16" {
    var buffer: [100]u8 = undefined;
    inline for (int_types) |T| {
        const info = @typeInfo(T).Int;
        if (info.signedness == .signed and info.bits >= 16) {
            var stream = std.io.fixedBufferStream(&buffer);
            var packer = msgpack.packer(stream.writer(), .{});
            try packer.writeInt(T, -20000);
            try std.testing.expectEqualSlices(u8, &.{ 0xd1, 0xb1, 0xe0 }, stream.getWritten());
        }
    }
}

test "writeInt: positive int32" {
    var buffer: [100]u8 = undefined;
    inline for (int_types) |T| {
        const info = @typeInfo(T).Int;
        if (info.signedness == .signed and info.bits >= 32) {
            var stream = std.io.fixedBufferStream(&buffer);
            var packer = msgpack.packer(stream.writer(), .{});
            try packer.writeInt(T, 2000000000);
            try std.testing.expectEqualSlices(u8, &.{ 0xd2, 0x77, 0x35, 0x94, 0x0 }, stream.getWritten());
        }
    }
}

test "writeInt: negative int32" {
    var buffer: [100]u8 = undefined;
    inline for (int_types) |T| {
        const info = @typeInfo(T).Int;
        if (info.signedness == .signed and info.bits >= 32) {
            var stream = std.io.fixedBufferStream(&buffer);
            var packer = msgpack.packer(stream.writer(), .{});
            try packer.writeInt(T, -2000000000);
            try std.testing.expectEqualSlices(u8, &.{ 0xd2, 0x88, 0xca, 0x6c, 0x00 }, stream.getWritten());
        }
    }
}

test "writeInt: positive int64" {
    var buffer: [100]u8 = undefined;
    inline for (int_types) |T| {
        const info = @typeInfo(T).Int;
        if (info.signedness == .signed and info.bits >= 64) {
            var stream = std.io.fixedBufferStream(&buffer);
            var packer = msgpack.packer(stream.writer(), .{});
            try packer.writeInt(T, 8000000000000000000);
            try std.testing.expectEqualSlices(u8, &.{ 0xd3, 0x6f, 0x5, 0xb5, 0x9d, 0x3b, 0x20, 0x0, 0x0 }, stream.getWritten());
        }
    }
}

test "writeInt: negative int64" {
    var buffer: [100]u8 = undefined;
    inline for (int_types) |T| {
        const info = @typeInfo(T).Int;
        if (info.signedness == .signed and info.bits >= 64) {
            var stream = std.io.fixedBufferStream(&buffer);
            var packer = msgpack.packer(stream.writer(), .{});
            try packer.writeInt(T, -9000000000000000000);
            try std.testing.expectEqualSlices(u8, &.{ 0xd3, 0x83, 0x19, 0x93, 0xaf, 0x1d, 0x7c, 0x0, 0x0 }, stream.getWritten());
        }
    }
}
