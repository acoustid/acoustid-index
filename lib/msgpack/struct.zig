const std = @import("std");
const hdrs = @import("headers.zig");

const NonOptional = @import("utils.zig").NonOptional;
const Optional = @import("utils.zig").Optional;
const isOptional = @import("utils.zig").isOptional;

const NoAllocator = @import("utils.zig").NoAllocator;

const maybePackNull = @import("null.zig").maybePackNull;
const maybeUnpackNull = @import("null.zig").maybeUnpackNull;

const packMapHeader = @import("map.zig").packMapHeader;
const unpackMapHeader = @import("map.zig").unpackMapHeader;

const packInt = @import("int.zig").packInt;
const unpackInt = @import("int.zig").unpackInt;

const packString = @import("string.zig").packString;
const unpackStringInto = @import("string.zig").unpackStringInto;

const packArrayHeader = @import("array.zig").packArrayHeader;
const unpackArrayHeader = @import("array.zig").unpackArrayHeader;

const packAny = @import("any.zig").packAny;
const unpackAny = @import("any.zig").unpackAny;

const packer = @import("msgpack.zig").packer;
const unpacker = @import("msgpack.zig").unpacker;

pub const StructAsMapOptions = struct {
    key: union(enum) {
        field_name,
        field_name_prefix: u8,
        field_index,
    },
    omit_nulls: bool = true,
    omit_defaults: bool = false,
};

pub const StructAsArrayOptions = struct {};

pub const StructFormat = union(enum) {
    as_map: StructAsMapOptions,
    as_array: StructAsArrayOptions,
};

pub const default_struct_format = StructFormat{
    .as_map = .{
        .key = .field_name,
    },
};

fn isStructFieldUsed(field: std.builtin.Type.StructField, value: anytype, opts: StructAsMapOptions) bool {
    const field_type_info = @typeInfo(field.type);
    const field_value = @field(value, field.name);

    if (opts.omit_defaults) {
        if (field.default_value) |default_field_value_ptr| {
            const default_field_value = @as(*field.type, @ptrCast(@alignCast(@constCast(default_field_value_ptr)))).*;
            if (field_value == default_field_value) {
                return false;
            }
        }
    }

    if (opts.omit_nulls) {
        if (field_type_info == .Optional) {
            if (field_value == null) {
                return false;
            }
        }
    }

    return true;
}

fn countUsedStructFields(fields: []const std.builtin.Type.StructField, value: anytype, opts: StructAsMapOptions) u16 {
    var used_field_count: u16 = 0;
    inline for (fields) |field| {
        if (isStructFieldUsed(field, value, opts)) {
            used_field_count += 1;
        }
    }
    return used_field_count;
}

fn strPrefix(src: []const u8, len: usize) []const u8 {
    return src[0..@min(src.len, len)];
}

pub fn packStructAsMap(writer: anytype, comptime T: type, value: T, opts: StructAsMapOptions) !void {
    const type_info = @typeInfo(T);
    const fields = type_info.Struct.fields;

    try packMapHeader(writer, countUsedStructFields(fields, value, opts));

    inline for (fields, 0..) |field, i| {
        if (isStructFieldUsed(field, value, opts)) {
            switch (opts.key) {
                .field_index => {
                    try packInt(writer, u16, i);
                },
                .field_name => {
                    try packString(writer, field.name);
                },
                .field_name_prefix => |prefix| {
                    try packString(writer, strPrefix(field.name, prefix));
                },
            }
            try packAny(writer, field.type, @field(value, field.name));
        }
    }
}

pub fn packStructAsArray(writer: anytype, comptime T: type, value: T, opts: StructAsArrayOptions) !void {
    const type_info = @typeInfo(T);
    const fields = type_info.Struct.fields;

    try packArrayHeader(writer, fields.len);

    inline for (fields) |field| {
        try packAny(writer, field.type, @field(value, field.name));
    }

    _ = opts;
}

pub fn packStruct(writer: anytype, comptime T: type, value_or_maybe_null: T) !void {
    const value = try maybePackNull(writer, T, value_or_maybe_null) orelse return;
    const Type = @TypeOf(value);
    const type_info = @typeInfo(Type);

    if (type_info != .Struct) {
        @compileError("Expected struct type");
    }

    const has_custom_write_fn = std.meta.hasFn(Type, "msgpackWrite");
    if (has_custom_write_fn) {
        return try value.msgpackWrite(packer(writer));
    } else {
        const format = if (std.meta.hasFn(Type, "msgpackFormat")) T.msgpackFormat() else default_struct_format;
        switch (format) {
            .as_map => |opts| {
                return packStructAsMap(writer, Type, value, opts);
            },
            .as_array => |opts| {
                return packStructAsArray(writer, Type, value, opts);
            },
        }
    }
}

pub fn unpackStructAsMap(reader: anytype, allocator: std.mem.Allocator, comptime T: type, opts: StructAsMapOptions) !T {
    const len = if (@typeInfo(T) == .Optional)
        try unpackMapHeader(reader, ?u16) orelse return null
    else
        try unpackMapHeader(reader, u16);

    const Type = NonOptional(T);
    const type_info = @typeInfo(Type);
    const fields = type_info.Struct.fields;

    var fields_seen = std.bit_set.StaticBitSet(fields.len).initEmpty();

    var field_name_buffer: [256]u8 = undefined;

    var result: Type = undefined;

    for (0..len) |_| {
        var field_index: u16 = undefined;
        switch (opts.key) {
            .field_index => {
                field_index = try unpackInt(reader, u16);
                inline for (fields, 0..) |field, i| {
                    if (field_index == i) {
                        fields_seen.set(i);
                        @field(result, field.name) = try unpackAny(reader, allocator, field.type);
                        break;
                    }
                } else {
                    return error.UnknownStructField;
                }
            },
            .field_name => {
                const field_name = try unpackStringInto(reader, &field_name_buffer);
                inline for (fields, 0..) |field, i| {
                    if (std.mem.eql(u8, field.name, field_name)) {
                        fields_seen.set(i);
                        @field(result, field.name) = try unpackAny(reader, allocator, field.type);
                        break;
                    }
                } else {
                    return error.UnknownStructField;
                }
            },
            .field_name_prefix => |prefix| {
                const field_name = try unpackStringInto(reader, &field_name_buffer);
                inline for (fields, 0..) |field, i| {
                    if (std.mem.startsWith(u8, field.name, strPrefix(field_name, prefix))) {
                        fields_seen.set(i);
                        @field(result, field.name) = try unpackAny(reader, allocator, field.type);
                        break;
                    }
                } else {
                    return error.UnknownStructField;
                }
            },
        }
    }

    inline for (fields, 0..) |field, i| {
        if (!fields_seen.isSet(i)) {
            if (field.default_value) |default_field_value_ptr| {
                const default_field_value = @as(*field.type, @ptrCast(@alignCast(@constCast(default_field_value_ptr)))).*;
                @field(result, field.name) = default_field_value;
                fields_seen.set(i);
            } else if (@typeInfo(field.type) == .Optional) {
                @field(result, field.name) = null;
                fields_seen.set(i);
            }
        }
    }

    if (fields_seen.count() != fields.len) {
        return error.MissingStructFields;
    }

    return result;
}

pub fn unpackStructAsArray(reader: anytype, allocator: std.mem.Allocator, comptime T: type, opts: StructAsArrayOptions) !T {
    const len = if (@typeInfo(T) == .Optional)
        try unpackArrayHeader(reader, ?u16) orelse return null
    else
        try unpackArrayHeader(reader, u16);

    const Type = NonOptional(T);
    const type_info = @typeInfo(Type);
    const fields = type_info.Struct.fields;

    if (len != fields.len) {
        return error.InvalidFormat;
    }

    var result: Type = undefined;

    inline for (fields) |field| {
        @field(result, field.name) = try unpackAny(reader, allocator, field.type);
    }

    _ = opts;
    return result;
}

pub fn unpackStruct(reader: anytype, allocator: std.mem.Allocator, comptime T: type) !T {
    const Type = NonOptional(T);

    const has_custom_read_fn = std.meta.hasFn(Type, "msgpackRead");
    if (has_custom_read_fn) {
        return try T.msgpackRead(unpacker(reader, allocator));
    } else {
        const format = if (std.meta.hasFn(Type, "msgpackFormat")) T.msgpackFormat() else default_struct_format;
        switch (format) {
            .as_map => |opts| {
                return try unpackStructAsMap(reader, allocator, T, opts);
            },
            .as_array => |opts| {
                return try unpackStructAsArray(reader, allocator, T, opts);
            },
        }
    }
}

test "writeStruct: map_by_index" {
    const Msg = struct {
        a: u32,
        b: u64,

        pub fn msgpackFormat() StructFormat {
            return .{ .as_map = .{ .key = .field_index } };
        }
    };
    const msg = Msg{ .a = 1, .b = 2 };

    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packStruct(stream.writer(), Msg, msg);

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

        pub fn msgpackFormat() StructFormat {
            return .{ .as_map = .{ .key = .field_name } };
        }
    };
    const msg = Msg{ .a = 1, .b = 2 };

    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packStruct(stream.writer(), Msg, msg);

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

        pub fn msgpackFormat() StructFormat {
            return .{ .as_array = .{} };
        }
    };
    const msg = Msg{ .a = 1, .b = 2 };

    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packStruct(stream.writer(), Msg, msg);

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

        pub fn msgpackFormat() StructFormat {
            return .{ .as_map = .{ .key = .field_index, .omit_defaults = true } };
        }
    };
    const msg = Msg{ .a = 1, .b = 2 };

    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packStruct(stream.writer(), Msg, msg);

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

        pub fn msgpackFormat() StructFormat {
            return .{ .as_map = .{ .key = .field_index, .omit_nulls = true } };
        }
    };
    const msg = Msg{ .a = null, .b = 2 };

    var buffer: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try packStruct(stream.writer(), Msg, msg);

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

        pub fn msgpackFormat() StructFormat {
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
    const value = try unpackStruct(stream.reader(), NoAllocator.allocator(), Msg);
    try std.testing.expectEqual(Msg{ .a = 1, .b = 2 }, value);
}

test "readStruct: map_by_name" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const Msg = struct {
        a: u32,
        b: u64,

        pub fn msgpackFormat() StructFormat {
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
    const value = try unpackStruct(stream.reader(), NoAllocator.allocator(), Msg);
    try std.testing.expectEqual(Msg{ .a = 1, .b = 2 }, value);
}

test "readStruct: array" {
    const Msg = struct {
        a: u32,
        b: u64,

        pub fn msgpackFormat() StructFormat {
            return .{ .as_array = .{} };
        }
    };

    const buffer = [_]u8{
        0x92, // array with 2 elements
        0x01, // value: u32(1)
        0x02, // value: i32(2)
    };
    var stream = std.io.fixedBufferStream(&buffer);
    const value = try unpackStruct(stream.reader(), NoAllocator.allocator(), Msg);
    try std.testing.expectEqual(Msg{ .a = 1, .b = 2 }, value);
}

test "readStruct: omit nulls" {
    const Msg = struct {
        a: ?u32,
        b: ?u64,

        pub fn msgpackFormat() StructFormat {
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
    const value = try unpackStruct(stream.reader(), NoAllocator.allocator(), Msg);
    try std.testing.expectEqual(Msg{ .a = 1, .b = null }, value);
}

test "readStruct: omit defaults" {
    const Msg = struct {
        a: u32,
        b: u64 = 100,

        pub fn msgpackFormat() StructFormat {
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
    const value = try unpackStruct(stream.reader(), NoAllocator.allocator(), Msg);
    try std.testing.expectEqual(Msg{ .a = 1, .b = 100 }, value);
}

test "readStruct: missing field" {
    const Msg = struct {
        a: u32,
        b: u64,

        pub fn msgpackFormat() StructFormat {
            return .{ .as_map = .{ .key = .field_index } };
        }
    };

    const buffer = [_]u8{
        0x81, // map with 2 elements
        0x00, // 0
        0x01, // value: u32(1)
    };
    var stream = std.io.fixedBufferStream(&buffer);
    const value = unpackStruct(stream.reader(), NoAllocator.allocator(), Msg);
    try std.testing.expectError(error.MissingStructFields, value);
}
