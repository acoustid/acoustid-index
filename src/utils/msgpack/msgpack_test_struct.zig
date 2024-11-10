const std = @import("std");
const msgpack = @import("msgpack.zig");

test "writeStruct: map_by_index" {
    const Msg = struct {
        a: u32,
        b: u64,
    };
    const msg = Msg{ .a = 1, .b = 2 };

    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    var packer = msgpack.packer(stream.writer(), .{ .struct_format = .map_by_index });
    try packer.writeStruct(Msg, msg);

    try std.testing.expectEqualSlices(u8, &.{
        0x82, // map with 2 elements
        0x00, // key: fixint 0
        0x01, // value: u32(1)
        0x01, // key: fixint 1
        0x02, // value: i32(2)
    }, stream.getWritten());
}

test "writeStruct: map_by_name" {
    const Msg = struct {
        a: u32,
        b: u64,
    };
    const msg = Msg{ .a = 1, .b = 2 };

    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    var packer = msgpack.packer(stream.writer(), .{ .struct_format = .map_by_name });
    try packer.writeStruct(Msg, msg);

    try std.testing.expectEqualSlices(u8, &.{
        0x82, // map with 2 elements
        0xa1, 'a', // "a"
        0x01, // value: u32(1)
        0xa1, 'b', // "b"
        0x02, // value: i32(2)
    }, stream.getWritten());
}

test "writeStruct: array" {
    const Msg = struct {
        a: u32,
        b: u64,
    };
    const msg = Msg{ .a = 1, .b = 2 };

    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    var packer = msgpack.packer(stream.writer(), .{ .struct_format = .array });
    try packer.writeStruct(Msg, msg);

    try std.testing.expectEqualSlices(u8, &.{
        0x92, // array with 2 elements
        0x01, // value: u32(1)
        0x02, // value: i32(2)
    }, stream.getWritten());
}

test "writeStruct: omit defaults" {
    const Msg = struct {
        a: u32 = 1,
        b: u64,
    };
    const msg = Msg{ .a = 1, .b = 2 };

    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    var packer = msgpack.packer(stream.writer(), .{ .struct_format = .map_by_index, .omit_defaults = true });
    try packer.writeStruct(Msg, msg);

    try std.testing.expectEqualSlices(u8, &.{
        0x81, // map with 1 element
        0x01, // key: fixint 1
        0x02, // value: i32(2)
    }, stream.getWritten());
}

test "writeStruct: omit nulls" {
    const Msg = struct {
        a: ?u32,
        b: u64,
    };
    const msg = Msg{ .a = null, .b = 2 };

    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    var packer = msgpack.packer(stream.writer(), .{ .struct_format = .map_by_index, .omit_nulls = true });
    try packer.writeStruct(Msg, msg);

    try std.testing.expectEqualSlices(u8, &.{
        0x81, // map with 1 element
        0x01, // key: fixint 1
        0x02, // value: i32(2)
    }, stream.getWritten());
}

test "readStruct: map_by_index" {
    const Msg = struct {
        a: u32,
        b: u64,
    };

    const buffer = [_]u8{
        0x82, // map with 2 elements
        0x00, // key: fixint 0
        0x01, // value: u32(1)
        0x01, // key: fixint 1
        0x02, // value: i32(2)
    };
    var stream = std.io.fixedBufferStream(&buffer);
    var unpacker = msgpack.unpackerNoAlloc(stream.reader(), .{ .struct_format = .map_by_index });
    try std.testing.expectEqual(Msg{ .a = 1, .b = 2 }, try unpacker.readStruct(Msg, .required));
}

test "readStruct: map_by_name" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const Msg = struct {
        a: u32,
        b: u64,
    };

    const buffer = [_]u8{
        0x82, // map with 2 elements
        0xa1, 'a', // "a"
        0x01, // value: u32(1)
        0xa1, 'b', // "b"
        0x02, // value: i32(2)
    };
    var stream = std.io.fixedBufferStream(&buffer);
    var unpacker = msgpack.unpacker(stream.reader(), arena.allocator(), .{ .struct_format = .map_by_name });
    try std.testing.expectEqual(Msg{ .a = 1, .b = 2 }, try unpacker.readStruct(Msg, .required));
}

test "readStruct: array" {
    const Msg = struct {
        a: u32,
        b: u64,
    };

    const buffer = [_]u8{
        0x92, // array with 2 elements
        0x01, // value: u32(1)
        0x02, // value: i32(2)
    };
    var stream = std.io.fixedBufferStream(&buffer);
    var unpacker = msgpack.unpackerNoAlloc(stream.reader(), .{ .struct_format = .array });
    try std.testing.expectEqual(Msg{ .a = 1, .b = 2 }, try unpacker.readStruct(Msg, .required));
}

test "readStruct: omit nulls" {
    const Msg = struct {
        a: ?u32,
        b: ?u64,
    };

    const buffer = [_]u8{
        0x81, // map with 2 elements
        0x00, // 0
        0x01, // value: u32(1)
    };
    var stream = std.io.fixedBufferStream(&buffer);
    var unpacker = msgpack.unpackerNoAlloc(stream.reader(), .{ .struct_format = .map_by_index });
    try std.testing.expectEqual(Msg{ .a = 1, .b = null }, try unpacker.readStruct(Msg, .required));
}

test "readStruct: omit defaults" {
    const Msg = struct {
        a: u32,
        b: u64 = 100,
    };

    const buffer = [_]u8{
        0x81, // map with 2 elements
        0x00, // 0
        0x01, // value: u32(1)
    };
    var stream = std.io.fixedBufferStream(&buffer);
    var unpacker = msgpack.unpackerNoAlloc(stream.reader(), .{ .struct_format = .map_by_index });
    try std.testing.expectEqual(Msg{ .a = 1, .b = 100 }, try unpacker.readStruct(Msg, .required));
}

test "readStruct: missing field" {
    const Msg = struct {
        a: u32,
        b: u64,
    };

    const buffer = [_]u8{
        0x81, // map with 2 elements
        0x00, // 0
        0x01, // value: u32(1)
    };
    var stream = std.io.fixedBufferStream(&buffer);
    var unpacker = msgpack.unpackerNoAlloc(stream.reader(), .{ .struct_format = .map_by_index });
    try std.testing.expectError(error.InvalidFormat, unpacker.readStruct(Msg, .required));
}
