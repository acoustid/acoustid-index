const std = @import("std");
const hdrs = @import("headers.zig");

const NonOptional = @import("utils.zig").NonOptional;
const Optional = @import("utils.zig").Optional;
const isOptional = @import("utils.zig").isOptional;

const maybePackNull = @import("null.zig").maybePackNull;
const maybeUnpackNull = @import("null.zig").maybeUnpackNull;

const packIntValue = @import("int.zig").packIntValue;
const unpackIntValue = @import("int.zig").unpackIntValue;

const packAny = @import("any.zig").packAny;
const unpackAny = @import("any.zig").unpackAny;

pub fn sizeOfPackedMapHeader(len: usize) !usize {
    if (len <= hdrs.FIXMAP_MAX - hdrs.FIXMAP_MIN) {
        return 1;
    } else if (len <= std.math.maxInt(u8)) {
        return 1 + @sizeOf(u8);
    } else if (len <= std.math.maxInt(u16)) {
        return 1 + @sizeOf(u16);
    } else if (len <= std.math.maxInt(u32)) {
        return 1 + @sizeOf(u32);
    } else {
        return error.MapTooLong;
    }
}

pub fn sizeOfPackedMap(len: usize) !usize {
    return try sizeOfPackedMapHeader(len) + len;
}

pub fn packMapHeader(writer: anytype, len: usize) !void {
    if (len <= hdrs.FIXMAP_MAX - hdrs.FIXMAP_MIN) {
        try writer.writeByte(hdrs.FIXMAP_MIN + @as(u8, @intCast(len)));
    } else if (len <= std.math.maxInt(u16)) {
        try writer.writeByte(hdrs.MAP16);
        try packIntValue(writer, u16, @intCast(len));
    } else if (len <= std.math.maxInt(u32)) {
        try writer.writeByte(hdrs.MAP32);
        try packIntValue(writer, u32, @intCast(len));
    } else {
        return error.MapTooLong;
    }
}

pub fn unpackMapHeader(reader: anytype, comptime T: type) !T {
    const header = try reader.readByte();
    switch (header) {
        hdrs.FIXMAP_MIN...hdrs.FIXMAP_MAX => {
            return @intCast(header - hdrs.FIXMAP_MIN);
        },
        hdrs.MAP16 => {
            return try unpackIntValue(reader, u16, NonOptional(T));
        },
        hdrs.MAP32 => {
            return try unpackIntValue(reader, u32, NonOptional(T));
        },
        else => {
            return maybeUnpackNull(header, T);
        },
    }
}

pub fn packMap(writer: anytype, value_or_maybe_null: anytype) !void {
    const value = try maybePackNull(writer, @TypeOf(value_or_maybe_null), value_or_maybe_null) orelse return;

    try packMapHeader(writer, value.count());

    var iter = value.iterator();
    while (iter.next()) |entry| {
        try packAny(writer, @TypeOf(entry.key_ptr.*), entry.key_ptr.*);
        try packAny(writer, @TypeOf(entry.value_ptr.*), entry.value_ptr.*);
    }
}

pub fn unpackMap(writer: anytype, allocator: std.mem.Allocator, comptime T: type) !T {
    var map = T.init(allocator);
    errdefer map.deinit();

    try unpackMapInto(writer, allocator, &map);

    return map;
}

pub fn unpackMapInto(writer: anytype, allocator: std.mem.Allocator, map: anytype) !void {
    const T = std.meta.Child(@TypeOf(map));
    const len = try unpackMapHeader(writer, T.Size);

    try map.ensureTotalCapacity(len);

    for (0..len) |_| {
        var kv: T.KV = undefined;
        kv.key = try unpackAny(writer, allocator, @TypeOf(kv.key));
        kv.value = try unpackAny(writer, allocator, @TypeOf(kv.value));
        map.putAssumeCapacity(kv.key, kv.value);
    }
}

test "packMap" {
    var map = std.AutoHashMap(u8, u8).init(std.testing.allocator);
    defer map.deinit();

    try map.put(1, 2);

    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    try packMap(buf.writer(), map);

    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x81, 0x01, 0x02 }, buf.items);
}

test "unpackMap" {
    var buf = [_]u8{ 0x81, 0x01, 0x02 };
    var stream = std.io.fixedBufferStream(&buf);

    var map = try unpackMap(stream.reader(), std.testing.allocator, std.AutoHashMap(u8, u8));
    defer map.deinit();

    try std.testing.expectEqual(2, map.get(1));
}

test "sizeOfPackedMap" {
    try std.testing.expectEqual(1, sizeOfPackedMap(0));
}
