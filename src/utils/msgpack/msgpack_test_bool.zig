const std = @import("std");
const msgpack = @import("msgpack.zig");

const packed_null = [_]u8{0xc0};
const packed_true = [_]u8{0xc3};
const packed_false = [_]u8{0xc2};
const packed_zero = [_]u8{0x00};

test "readBool: false" {
    try std.testing.expectEqual(false, try msgpack.unpackFromBytes(bool, &packed_false, .{}));
}

test "readBool: true" {
    try std.testing.expectEqual(true, try msgpack.unpackFromBytes(bool, &packed_true, .{}));
}

test "readBool: null" {
    try std.testing.expectEqual(null, try msgpack.unpackFromBytes(?bool, &packed_null, .{}));
}

test "readBool: wrong type" {
    try std.testing.expectError(error.InvalidFormat, msgpack.unpackFromBytes(bool, &packed_zero, .{}));
}

test "writeBool: false" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try msgpack.pack(bool, stream.writer(), false);
    try std.testing.expectEqualSlices(u8, &packed_false, stream.getWritten());
}

test "writeBool: true" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try msgpack.pack(bool, stream.writer(), true);
    try std.testing.expectEqualSlices(u8, &packed_true, stream.getWritten());
}

test "writeBool: null" {
    var buffer: [16]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try msgpack.pack(?bool, stream.writer(), null);
    try std.testing.expectEqualSlices(u8, &packed_null, stream.getWritten());
}
