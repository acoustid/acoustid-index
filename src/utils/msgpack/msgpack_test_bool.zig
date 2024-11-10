const std = @import("std");
const msgpack = @import("msgpack.zig");

test "readBool: null" {
    const buffer = [_]u8{0xc0};
    var stream = std.io.fixedBufferStream(&buffer);
    var unpacker = msgpack.unpackerNoAlloc(stream.reader(), .{});
    try std.testing.expectEqual(null, try unpacker.read(?bool));
}

test "readBool: false" {
    const buffer = [_]u8{0xc2};
    var stream = std.io.fixedBufferStream(&buffer);
    var unpacker = msgpack.unpackerNoAlloc(stream.reader(), .{});
    try std.testing.expectEqual(false, try unpacker.read(bool));
}

test "readBool: true" {
    const buffer = [_]u8{0xc3};
    var stream = std.io.fixedBufferStream(&buffer);
    var unpacker = msgpack.unpackerNoAlloc(stream.reader(), .{});
    try std.testing.expectEqual(true, try unpacker.read(bool));
}

test "readBool: wrong data" {
    const buffer = [_]u8{0x00};
    var stream = std.io.fixedBufferStream(&buffer);
    var unpacker = msgpack.unpackerNoAlloc(stream.reader(), .{});
    try std.testing.expectError(error.InvalidFormat, unpacker.read(bool));
}

test "writeBool: false" {
    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    var packer = msgpack.packer(stream.writer(), .{});
    try packer.writeBool(false);
    try std.testing.expectEqualSlices(u8, &.{0xc2}, stream.getWritten());
}

test "writeBool: true" {
    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    var packer = msgpack.packer(stream.writer(), .{});
    try packer.writeBool(true);
    try std.testing.expectEqualSlices(u8, &.{0xc3}, stream.getWritten());
}