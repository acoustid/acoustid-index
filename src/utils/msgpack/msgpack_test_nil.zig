const std = @import("std");
const msgpack = @import("msgpack.zig");

test "readNil" {
    const buffer = [_]u8{0xc0};
    var stream = std.io.fixedBufferStream(&buffer);
    var unpacker = msgpack.unpackerNoAlloc(stream.reader(), .{});
    try unpacker.readNil();
}

test "readNil: wrong data" {
    const buffer = [_]u8{0x00};
    var stream = std.io.fixedBufferStream(&buffer);
    var unpacker = msgpack.unpackerNoAlloc(stream.reader(), .{});
    try std.testing.expectError(error.InvalidFormat, unpacker.readNil());
}

test "writeNil" {
    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    var packer = msgpack.packer(stream.writer(), .{});
    try packer.writeNil();
    try std.testing.expectEqualSlices(u8, &.{0xc0}, stream.getWritten());
}
