const std = @import("std");
const msgpack = @import("msgpack.zig");

test "writeStruct: map_by_index" {
    const Msg = struct {
        a: u32,
        b: u64,

        pub fn msgpackFormat() msgpack.StructFormat {
            return .{ .as_map = .{ .key = .field_index } };
        }
    };
    const msg = Msg{ .a = 1, .b = 2 };

    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    var packer = msgpack.packer(stream.writer());
    try packer.writeStruct(Msg, msg, 0);

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

        pub fn msgpackFormat() msgpack.StructFormat {
            return .{ .as_map = .{ .key = .field_name } };
        }
    };
    const msg = Msg{ .a = 1, .b = 2 };

    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    var packer = msgpack.packer(stream.writer());
    try packer.writeStruct(Msg, msg, 0);

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

        pub fn msgpackFormat() msgpack.StructFormat {
            return .{ .as_array = .{} };
        }
    };
    const msg = Msg{ .a = 1, .b = 2 };

    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    var packer = msgpack.packer(stream.writer());
    try packer.writeStruct(Msg, msg, 0);

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

        pub fn msgpackFormat() msgpack.StructFormat {
            return .{ .as_map = .{ .key = .field_index, .omit_defaults = true } };
        }
    };
    const msg = Msg{ .a = 1, .b = 2 };

    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    var packer = msgpack.packer(stream.writer());
    try packer.writeStruct(Msg, msg, 0);

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

        pub fn msgpackFormat() msgpack.StructFormat {
            return .{ .as_map = .{ .key = .field_index, .omit_nulls = true } };
        }
    };
    const msg = Msg{ .a = null, .b = 2 };

    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    var packer = msgpack.packer(stream.writer());
    try packer.writeStruct(Msg, msg, 0);

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

        pub fn msgpackFormat() msgpack.StructFormat {
            return .{ .as_map = .{ .key = .field_index } };
        }
    };

    const buffer = [_]u8{
        0x82, // map with 2 elements
        0x00, // key: fixint 0
        0x01, // value: u32(1)
        0x01, // key: fixint 1
        0x02, // value: i32(2)
    };
    var stream = std.io.fixedBufferStream(&buffer);
    var unpacker = msgpack.unpackerNoAlloc(stream.reader());
    try std.testing.expectEqual(Msg{ .a = 1, .b = 2 }, try unpacker.read(Msg));
}

test "readStruct: map_by_name" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const Msg = struct {
        a: u32,
        b: u64,

        pub fn msgpackFormat() msgpack.StructFormat {
            return .{ .as_map = .{ .key = .field_name } };
        }
    };

    const buffer = [_]u8{
        0x82, // map with 2 elements
        0xa1, 'a', // "a"
        0x01, // value: u32(1)
        0xa1, 'b', // "b"
        0x02, // value: i32(2)
    };
    var stream = std.io.fixedBufferStream(&buffer);
    var unpacker = msgpack.unpacker(stream.reader(), arena.allocator());
    try std.testing.expectEqual(Msg{ .a = 1, .b = 2 }, try unpacker.read(Msg));
}

test "readStruct: array" {
    const Msg = struct {
        a: u32,
        b: u64,

        pub fn msgpackFormat() msgpack.StructFormat {
            return .{ .as_array = .{} };
        }
    };

    const buffer = [_]u8{
        0x92, // array with 2 elements
        0x01, // value: u32(1)
        0x02, // value: i32(2)
    };
    var stream = std.io.fixedBufferStream(&buffer);
    var unpacker = msgpack.unpackerNoAlloc(stream.reader());
    try std.testing.expectEqual(Msg{ .a = 1, .b = 2 }, try unpacker.read(Msg));
}

test "readStruct: omit nulls" {
    const Msg = struct {
        a: ?u32,
        b: ?u64,

        pub fn msgpackFormat() msgpack.StructFormat {
            return .{ .as_map = .{
                .key = .field_index,
                .omit_nulls = true,
            } };
        }
    };

    const buffer = [_]u8{
        0x81, // map with 2 elements
        0x00, // 0
        0x01, // value: u32(1)
    };
    var stream = std.io.fixedBufferStream(&buffer);
    var unpacker = msgpack.unpackerNoAlloc(stream.reader());
    try std.testing.expectEqual(Msg{ .a = 1, .b = null }, try unpacker.read(Msg));
}

test "readStruct: omit defaults" {
    const Msg = struct {
        a: u32,
        b: u64 = 100,

        pub fn msgpackFormat() msgpack.StructFormat {
            return .{ .as_map = .{
                .key = .field_index,
                .omit_defaults = true,
            } };
        }
    };

    const buffer = [_]u8{
        0x81, // map with 2 elements
        0x00, // 0
        0x01, // value: u32(1)
    };
    var stream = std.io.fixedBufferStream(&buffer);
    var unpacker = msgpack.unpackerNoAlloc(stream.reader());
    try std.testing.expectEqual(Msg{ .a = 1, .b = 100 }, try unpacker.read(Msg));
}

test "readStruct: missing field" {
    const Msg = struct {
        a: u32,
        b: u64,

        pub fn msgpackFormat() msgpack.StructFormat {
            return .{ .as_map = .{ .key = .field_index } };
        }
    };

    const buffer = [_]u8{
        0x81, // map with 2 elements
        0x00, // 0
        0x01, // value: u32(1)
    };
    var stream = std.io.fixedBufferStream(&buffer);
    var unpacker = msgpack.unpackerNoAlloc(stream.reader());
    try std.testing.expectError(error.InvalidFormat, unpacker.read(Msg));
}
