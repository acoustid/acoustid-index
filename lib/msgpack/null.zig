const std = @import("std");
const hdrs = @import("headers.zig");

const isOptional = @import("utils.zig").isOptional;
const NonOptional = @import("utils.zig").NonOptional;

pub fn getNullSize() usize {
    return 1;
}

pub fn packNull(writer: anytype) !void {
    try writer.writeByte(hdrs.NIL);
}

pub fn unpackNull(reader: anytype) !void {
    const header = try reader.readByte();
    _ = try maybeUnpackNull(header, ?void);
}

pub fn maybePackNull(writer: anytype, comptime T: type, value: T) !?NonOptional(T) {
    if (@typeInfo(T) == .Optional) {
        if (value == null) {
            try packNull(writer);
            return null;
        } else {
            return value;
        }
    }
    return value;
}

pub fn isNullError(err: anyerror) bool {
    return err == error.Null;
}

pub fn maybeUnpackNull(header: u8, comptime T: type) !T {
    switch (header) {
        hdrs.NIL => return if (isOptional(T)) null else error.Null,
        else => return error.InvalidFormat,
    }
}

const packed_null = [_]u8{0xc0};
const packed_zero = [_]u8{0x00};

test "packNull" {
    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packNull(stream.writer());
    try std.testing.expectEqualSlices(u8, &packed_null, stream.getWritten());
}

test "unpackNull" {
    var stream = std.io.fixedBufferStream(&packed_null);
    try unpackNull(stream.reader());
}

test "unpackNull: wrong data" {
    var stream = std.io.fixedBufferStream(&packed_zero);
    try std.testing.expectError(error.InvalidFormat, unpackNull(stream.reader()));
}

test "getMaxNullSize/getNullSize" {
    try std.testing.expectEqual(1, getNullSize());
}
