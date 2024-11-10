const std = @import("std");
const msgpack = @import("msgpack.zig");

const Msg1 = union(enum) {
    a: u32,
    b: u64,
};
const msg1 = Msg1{ .a = 1 };
const msg1_packed = [_]u8{
    0x81, // map with 1 elements
    0x00, // key: fixint 0
    0x01, // value: u32(1)
};

const Msg2 = union(enum) {
    a,
    b: u64,
};
const msg2 = Msg2{ .a = {} };
const msg2_packed = [_]u8{
    0x81, // map with 1 elements
    0x00, // key: fixint 0
    0xc0, // value: nil
};

test "writeUnion: int field" {
    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    var packer = msgpack.packer(stream.writer(), .{});
    try packer.writeUnion(Msg1, msg1);

    try std.testing.expectEqualSlices(u8, &msg1_packed, stream.getWritten());
}

test "writeUnion: void field" {
    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    var packer = msgpack.packer(stream.writer(), .{});
    try packer.writeUnion(Msg2, msg2);

    try std.testing.expectEqualSlices(u8, &msg2_packed, stream.getWritten());
}

test "readUnion: int field" {
    var stream = std.io.fixedBufferStream(&msg1_packed);
    var unpacker = msgpack.unpackerNoAlloc(stream.reader(), .{ .struct_format = .map_by_index });
    try std.testing.expectEqual(msg1, try unpacker.readUnion(Msg1));
}

test "readUnion: void field" {
    var stream = std.io.fixedBufferStream(&msg2_packed);
    var unpacker = msgpack.unpackerNoAlloc(stream.reader(), .{ .struct_format = .map_by_index });
    try std.testing.expectEqual(msg2, try unpacker.readUnion(Msg2));
}
