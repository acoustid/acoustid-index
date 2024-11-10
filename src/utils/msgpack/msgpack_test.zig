const std = @import("std");
const msgpack = @import("msgpack.zig");

const Packer = msgpack.Packer;
const Unpacker = msgpack.Unpacker;

test "writeString" {
    var buffer: [1000]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    const writer = stream.writer();

    var packer = Packer(@TypeOf(writer)).init(writer, .{});

    {
        stream.reset();
        try packer.writeString("");

        const data = stream.getWritten();
        try std.testing.expectEqualSlices(u8, data, &.{
            0xa0, // empty string
        });
    }

    {
        stream.reset();
        try packer.writeString("hello");

        const data = stream.getWritten();
        try std.testing.expectEqualSlices(u8, data, &.{
            0xa5, 0x68, 0x65, 0x6c, 0x6c, 0x6f, // "hello"
        });
    }

    {
        stream.reset();
        try packer.writeString("hello world!");

        const data = stream.getWritten();
        try std.testing.expectEqualSlices(u8, data, &.{
            0xac, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x21, // "hello world!"
        });
    }

    {
        stream.reset();
        const long_string = "x" ** 256;
        try packer.writeString(long_string);

        const data = stream.getWritten();
        try std.testing.expect(data[0] == 0xda); // str 16
        try std.testing.expect(data[1] == 0x01); // length high byte
        try std.testing.expect(data[2] == 0x00); // length low byte
        try std.testing.expectEqual(data.len, 259); // 3 bytes header + 256 bytes content
        try std.testing.expectEqualSlices(u8, data[3..], long_string);
    }

    {
        stream.reset();
        try packer.writeStringHeader(70000);

        const data = stream.getWritten();
        try std.testing.expect(data[0] == 0xdb); // str 32
        try std.testing.expect(data[1] == 0x00); // length highest byte
        try std.testing.expect(data[2] == 0x01); // length high byte
        try std.testing.expect(data[3] == 0x11); // length mid byte
        try std.testing.expect(data[4] == 0x70); // length low byte
        try std.testing.expectEqual(data.len, 5);
    }

    {
        stream.reset();
        const err = packer.writeStringHeader(std.math.maxInt(u64));
        try std.testing.expectError(error.StringTooLong, err);
    }
}

test "writeBinary/writeBinaryHeader" {
    var buffer: [1000]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    const writer = stream.writer();

    var packer = Packer(@TypeOf(writer)).init(writer, .{});

    {
        stream.reset();
        try packer.writeBinary(&[_]u8{ 0x01, 0x02, 0x03 });

        const data = stream.getWritten();
        try std.testing.expectEqualSlices(u8, data, &.{
            0xc4, 0x03, 0x01, 0x02, 0x03, // bin 8 format
        });
    }

    {
        stream.reset();
        const bin_data = [_]u8{0xFF} ** 256;
        try packer.writeBinary(&bin_data);

        const data = stream.getWritten();
        try std.testing.expect(data[0] == 0xc5); // bin 16
        try std.testing.expect(data[1] == 0x01); // length high byte
        try std.testing.expect(data[2] == 0x00); // length low byte
        try std.testing.expectEqual(data.len, 259); // 3 bytes header + 256 bytes content
        try std.testing.expectEqualSlices(u8, data[3..], &bin_data);
    }

    {
        stream.reset();
        try packer.writeBinaryHeader(70000);

        const data = stream.getWritten();
        try std.testing.expect(data[0] == 0xc6); // bin 32
        try std.testing.expect(data[1] == 0x00); // length highest byte
        try std.testing.expect(data[2] == 0x01); // length high byte
        try std.testing.expect(data[3] == 0x11); // length mid byte
        try std.testing.expect(data[4] == 0x70); // length low byte
        try std.testing.expectEqual(data.len, 5);
    }

    {
        stream.reset();
        const err = packer.writeBinaryHeader(std.math.maxInt(u64));
        try std.testing.expectError(error.BinaryTooLong, err);
    }
}

test "writeArray/writeArrayHeader" {
    var buffer: [1000]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    const writer = stream.writer();

    var packer = Packer(@TypeOf(writer)).init(writer, .{});

    {
        stream.reset();
        try packer.writeArray(u16, &[_]u16{});
        const data = stream.getWritten();
        try std.testing.expectEqualSlices(u8, data, &.{
            0x90, // fixarray with 0 elements
        });
    }

    {
        stream.reset();
        try packer.writeArray(u8, &[_]u8{ 1, 2, 3 });
        const data = stream.getWritten();
        try std.testing.expectEqualSlices(u8, data, &.{
            0x93, // fixarray with 3 elements
            0x01, // first element
            0x02, // second element
            0x03, // third element
        });
    }

    {
        stream.reset();
        try packer.writeArrayHeader(16);
        const data = stream.getWritten();
        try std.testing.expectEqualSlices(u8, data, &.{
            0xdc, // array 16
            0x00, // length high byte
            0x10, // length low byte
        });
    }

    {
        stream.reset();
        try packer.writeArrayHeader(70000);
        const data = stream.getWritten();
        try std.testing.expectEqualSlices(u8, data, &.{
            0xdd, // array 32
            0x00, // length highest byte
            0x01, // length high byte
            0x11, // length mid byte
            0x70, // length low byte
        });
    }

    {
        stream.reset();
        const err = packer.writeArrayHeader(std.math.maxInt(u64));
        try std.testing.expectError(error.ArrayTooLong, err);
    }
}

test {
    _ = @import("msgpack_test_nil.zig");
    _ = @import("msgpack_test_bool.zig");
    _ = @import("msgpack_test_int.zig");
    _ = @import("msgpack_test_float.zig");
    _ = @import("msgpack_test_union.zig");
    _ = @import("msgpack_test_struct.zig");
}
