const std = @import("std");
const Allocator = std.mem.Allocator;

const Nullable = enum {
    optional,
    required,
};

fn NullableType(comptime T: type, comptime nullable: Nullable) type {
    switch (nullable) {
        .optional => return ?T,
        .required => return T,
    }
}

fn strPrefix(src: []const u8, len: usize) []const u8 {
    return src[0..@min(src.len, len)];
}

const MSG_POSITIVE_FIXINT_MIN = 0x00;
const MSG_POSITIVE_FIXINT_MAX = 0x7f;
const MSG_FIXMAP_MIN = 0x80;
const MSG_FIXMAP_MAX = 0x8f;
const MSG_FIXARRAY_MIN = 0x90;
const MSG_FIXARRAY_MAX = 0x9f;
const MSG_FIXSTR_MIN = 0xa0;
const MSG_FIXSTR_MAX = 0xbf;
const MSG_NIL = 0xc0;
const MSG_FALSE = 0xc2;
const MSG_TRUE = 0xc3;
const MSG_BIN8 = 0xc4;
const MSG_BIN16 = 0xc5;
const MSG_BIN32 = 0xc6;
const MSG_EXT8 = 0xc7;
const MSG_EXT16 = 0xc8;
const MSG_EXT32 = 0xc9;
const MSG_FLOAT32 = 0xca;
const MSG_FLOAT64 = 0xcb;
const MSG_UINT8 = 0xcc;
const MSG_UINT16 = 0xcd;
const MSG_UINT32 = 0xce;
const MSG_UINT64 = 0xcf;
const MSG_INT8 = 0xd0;
const MSG_INT16 = 0xd1;
const MSG_INT32 = 0xd2;
const MSG_INT64 = 0xd3;
const MSG_FIXEXT1 = 0xd4;
const MSG_FIXEXT2 = 0xd5;
const MSG_FIXEXT4 = 0xd6;
const MSG_FIXEXT8 = 0xd7;
const MSG_FIXEXT16 = 0xd8;
const MSG_STR8 = 0xd9;
const MSG_STR16 = 0xda;
const MSG_STR32 = 0xdb;
const MSG_ARRAY16 = 0xdc;
const MSG_ARRAY32 = 0xdd;
const MSG_MAP16 = 0xde;
const MSG_MAP32 = 0xdf;
const MSG_NEGATIVE_FIXINT_MIN = 0xe0;
const MSG_NEGATIVE_FIXINT_MAX = 0xff;

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

pub const UnionFormat = union(enum) {
    as_map: StructAsMapOptions,
};

pub const default_union_format = UnionFormat{
    .as_map = .{
        .key = .field_name,
    },
};

const NoAllocator = struct {};

pub const getNullSize = @import("null.zig").getNullSize;
pub const packNull = @import("null.zig").packNull;
pub const unpackNull = @import("null.zig").unpackNull;

pub const getBoolSize = @import("bool.zig").getBoolSize;
pub const packBool = @import("bool.zig").packBool;
pub const unpackBool = @import("bool.zig").unpackBool;

pub const getIntSize = @import("int.zig").getIntSize;
pub const getMaxIntSize = @import("int.zig").getMaxIntSize;
pub const packInt = @import("int.zig").packInt;
pub const packIntValue = @import("int.zig").packIntValue;
pub const unpackInt = @import("int.zig").unpackInt;

pub const getFloatSize = @import("float.zig").getFloatSize;
pub const getMaxFloatSize = @import("float.zig").getMaxFloatSize;
pub const packFloat = @import("float.zig").packFloat;
pub const unpackFloat = @import("float.zig").unpackFloat;

pub fn Packer(comptime Writer: type) type {
    return struct {
        writer: Writer,

        const Self = @This();

        pub fn init(writer: Writer) Self {
            return Self{
                .writer = writer,
            };
        }

        pub fn writeNull(self: Self) !void {
            try packNull(self.writer);
        }

        pub fn writeBool(self: Self, comptime T: type, value: T) !void {
            try packBool(self.writer, T, value);
        }

        pub fn writeInt(self: Self, comptime T: type, value: T) !void {
            try packInt(self.writer, T, value);
        }

        pub fn writeFloat(self: Self, comptime T: type, value: T) !void {
            return packFloat(self.writer, T, value);
        }

        pub fn getStringHeaderSize(len: usize) !usize {
            if (len <= MSG_FIXSTR_MAX - MSG_FIXARRAY_MIN) {
                return 1;
            } else if (len <= std.math.maxInt(u8)) {
                return 1 + @sizeOf(u8);
            } else if (len <= std.math.maxInt(u16)) {
                return 1 + @sizeOf(u16);
            } else if (len <= std.math.maxInt(u32)) {
                return 1 + @sizeOf(u32);
            } else {
                return error.StringTooLong;
            }
        }

        pub fn writeStringHeader(self: Self, len: usize) !void {
            if (len <= MSG_FIXSTR_MAX - MSG_FIXARRAY_MIN) {
                try self.writer.writeByte(MSG_FIXSTR_MIN + @as(u8, @intCast(len)));
            } else if (len <= std.math.maxInt(u8)) {
                try self.writer.writeByte(MSG_STR8);
                try packIntValue(self.writer, u8, @intCast(len));
            } else if (len <= std.math.maxInt(u16)) {
                try self.writer.writeByte(MSG_STR16);
                try packIntValue(self.writer, u16, @intCast(len));
            } else if (len <= std.math.maxInt(u32)) {
                try self.writer.writeByte(MSG_STR32);
                try packIntValue(self.writer, u32, @intCast(len));
            } else {
                return error.StringTooLong;
            }
        }

        pub fn getStringSize(value: []const u8) !usize {
            var size: usize = 0;
            size += try getStringHeaderSize(value.len);
            size += value.len;
            return size;
        }

        pub fn writeString(self: Self, value: []const u8) !void {
            try self.writeStringHeader(value.len);
            try self.writer.writeAll(value);
        }

        pub fn writeBinaryHeader(self: Self, len: usize) !void {
            if (len <= std.math.maxInt(u8)) {
                try self.writer.writeByte(MSG_BIN8);
                try packIntValue(self.writer, u8, @intCast(len));
            } else if (len <= std.math.maxInt(u16)) {
                try self.writer.writeByte(MSG_BIN16);
                try packIntValue(self.writer, u16, @intCast(len));
            } else if (len <= std.math.maxInt(u32)) {
                try self.writer.writeByte(MSG_BIN32);
                try packIntValue(self.writer, u32, @intCast(len));
            } else {
                return error.BinaryTooLong;
            }
        }

        pub fn writeBinary(self: Self, value: []const u8) !void {
            try self.writeBinaryHeader(value.len);
            try self.writer.writeAll(value);
        }

        pub fn getArrayHeaderSize(len: usize) !usize {
            if (len <= MSG_FIXARRAY_MAX - MSG_FIXARRAY_MIN) {
                return 1;
            } else if (len <= std.math.maxInt(u16)) {
                return 1 + @sizeOf(u16);
            } else if (len <= std.math.maxInt(u32)) {
                return 1 + @sizeOf(u32);
            } else {
                return error.ArrayTooLong;
            }
        }

        pub fn writeArrayHeader(self: Self, len: usize) !void {
            if (len <= MSG_FIXARRAY_MAX - MSG_FIXARRAY_MIN) {
                try self.writer.writeByte(MSG_FIXARRAY_MIN + @as(u8, @intCast(len)));
            } else if (len <= std.math.maxInt(u16)) {
                try self.writer.writeByte(MSG_ARRAY16);
                try packIntValue(self.writer, u16, @intCast(len));
            } else if (len <= std.math.maxInt(u32)) {
                try self.writer.writeByte(MSG_ARRAY32);
                try packIntValue(self.writer, u32, @intCast(len));
            } else {
                return error.ArrayTooLong;
            }
        }

        pub fn writeArray(self: Self, comptime T: type, value: []const T) !void {
            try self.writeArrayHeader(value.len);
            for (value) |item| {
                try self.write(T, item);
            }
        }

        pub fn writeArrayList(self: Self, comptime T: type, value: std.ArrayList(T)) !void {
            try self.writeArrayHeader(value.items.len);
            for (value.items) |item| {
                try self.write(T, item);
            }
        }

        pub fn getMaxMapHeaderSize() usize {
            return 1 + @sizeOf(u32);
        }

        pub fn getMapHeaderSize(len: usize) !usize {
            if (len <= MSG_FIXMAP_MAX - MSG_FIXMAP_MIN) {
                return 1;
            } else if (len <= std.math.maxInt(u16)) {
                return 1 + @sizeOf(u16);
            } else if (len <= std.math.maxInt(u32)) {
                return 1 + @sizeOf(u32);
            } else {
                return error.MapTooLong;
            }
        }

        pub fn writeMapHeader(self: Self, len: usize) !void {
            if (len <= MSG_FIXMAP_MAX - MSG_FIXMAP_MIN) {
                try self.writer.writeByte(MSG_FIXMAP_MIN + @as(u8, @intCast(len)));
            } else if (len <= std.math.maxInt(u16)) {
                try self.writer.writeByte(MSG_MAP16);
                try packIntValue(self.writer, u16, @intCast(len));
            } else if (len <= std.math.maxInt(u32)) {
                try self.writer.writeByte(MSG_MAP32);
                try packIntValue(self.writer, u32, @intCast(len));
            } else {
                return error.MapTooLong;
            }
        }

        fn isStructFieldUsed(opts: StructAsMapOptions, field: std.builtin.Type.StructField, value: anytype) bool {
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

        fn countUsedStructFields(opts: StructAsMapOptions, fields: []const std.builtin.Type.StructField, value: anytype) u16 {
            var used_field_count: u16 = 0;
            inline for (fields) |field| {
                if (isStructFieldUsed(opts, field, value)) {
                    used_field_count += 1;
                }
            }
            return used_field_count;
        }

        pub fn getMaxStructSize(comptime T: type, comptime extra_fields: u16) usize {
            const type_info = @typeInfo(T);
            if (type_info != .Struct) {
                @compileError("Expected struct type");
            }

            const fields = type_info.Struct.fields;
            if (fields.len > 255) {
                @compileError("Too many fields");
            }

            comptime var size: usize = 0;

            const format = if (std.meta.hasFn(T, "msgpackFormat")) T.msgpackFormat() else default_struct_format;
            switch (format) {
                .as_map => |opts| {
                    size += try getMapHeaderSize(fields.len + extra_fields);
                    inline for (fields, 0..) |field, i| {
                        switch (opts.key) {
                            .field_index => {
                                size += try getIntSize(u8, @intCast(i));
                            },
                            .field_name => {
                                size += try getStringSize(field.name);
                            },
                            .field_name_prefix => |prefix| {
                                size += try getStringSize(strPrefix(field.name, prefix));
                            },
                        }
                        size += getMaxSize(field.type);
                    }
                },
                .as_array => {
                    size += try getArrayHeaderSize(fields.len + extra_fields);
                    inline for (fields) |field| {
                        size += getMaxSize(field.type);
                    }
                },
            }

            return size;
        }

        pub fn writeStruct(self: Self, comptime T: type, value: T, comptime extra_fields: i16) !void {
            const type_info = @typeInfo(T);
            if (type_info != .Struct) {
                @compileError("Expected struct type");
            }

            const fields = type_info.Struct.fields;
            if (fields.len > 255) {
                @compileError("Too many fields");
            }

            const format = if (std.meta.hasFn(T, "msgpackFormat")) T.msgpackFormat() else default_struct_format;
            switch (format) {
                .as_map => |opts| {
                    try self.writeMapHeader(countUsedStructFields(opts, fields, value) + extra_fields);
                    inline for (fields, 0..) |field, i| {
                        if (isStructFieldUsed(opts, field, value)) {
                            switch (opts.key) {
                                .field_index => {
                                    try self.writeInt(u8, @intCast(i));
                                },
                                .field_name => {
                                    try self.writeString(field.name);
                                },
                                .field_name_prefix => |prefix| {
                                    try self.writeString(strPrefix(field.name, prefix));
                                },
                            }
                            try self.write(field.type, @field(value, field.name));
                        }
                    }
                },
                .as_array => {
                    try self.writeArrayHeader(fields.len + extra_fields);
                    inline for (fields) |field| {
                        try self.write(field.type, @field(value, field.name));
                    }
                },
            }
        }

        pub fn writeUnion(self: Self, comptime T: type, value: T) !void {
            const type_info = @typeInfo(T);
            if (type_info != .Union) {
                @compileError("Expected union type , not " ++ @typeName(T));
            }

            const TagType = type_info.Union.tag_type orelse @compileError("Unable to write untagged union '" ++ @typeName(T) ++ "'");

            const format: UnionFormat = if (std.meta.hasFn(T, "msgpackFormat")) T.msgpackFormat() else default_union_format;
            switch (format) {
                .as_map => |opts| {
                    try self.writeMapHeader(1);
                    inline for (type_info.Union.fields, 0..) |field, i| {
                        if (value == @field(TagType, field.name)) {
                            switch (opts.key) {
                                .field_index => {
                                    try self.writeInt(u8, @intCast(i));
                                },
                                .field_name => {
                                    try self.writeString(field.name);
                                },
                                .field_name_prefix => |prefix| {
                                    try self.writeString(strPrefix(field.name, prefix));
                                },
                            }
                            if (field.type == void) {
                                try self.writeNull();
                            } else {
                                try self.write(field.type, @field(value, field.name));
                            }
                            break;
                        }
                    } else {
                        unreachable;
                    }
                },
            }
        }

        pub fn writePointer(self: Self, comptime T: type, value: T) !void {
            const type_info = @typeInfo(T);
            if (type_info != .Pointer) {
                @compileError("Expected pointer type , not " ++ @typeName(T));
            }

            if (type_info.Pointer.size == .Slice) {
                try self.writeArray(type_info.Pointer.child, value);
            } else {
                @compileError("Unsupported pointer type " ++ @typeName(T));
            }
        }

        pub fn getMaxSize(comptime T: type) usize {
            const type_info = @typeInfo(T);
            switch (type_info) {
                .Bool => return getBoolSize(),
                .Int => return getMaxIntSize(T),
                .Float => return getMaxFloatSize(T),
                .Struct => return getMaxStructSize(T),
                .Optional => {
                    return getMaxSize(type_info.Optional.child);
                },
                else => @compileError("Unsupported type '" ++ @typeName(T) ++ "'"),
            }
        }

        pub fn write(self: Self, comptime T: type, value: T) !void {
            const type_info = @typeInfo(T);
            switch (type_info) {
                .Bool => try self.writeBool(T, value),
                .Int => try self.writeInt(T, value),
                .Float => try self.writeFloat(T, value),
                .Optional => {
                    if (value) |val| {
                        try self.write(type_info.Optional.child, val);
                    } else {
                        try self.writeNull();
                    }
                },
                .Array => try self.writeArray(type_info.Array.child, &value),
                .Struct => try self.writeStruct(T, value, 0),
                .Union => try self.writeUnion(T, value),
                .Pointer => try self.writePointer(T, value),
                else => @compileError("Unsupported type " ++ @typeName(T)),
            }
        }
    };
}

pub fn Unpacker(comptime Reader: type, comptime AllocatorType: type) type {
    return struct {
        reader: Reader,
        allocator: AllocatorType,

        const Self = @This();

        pub fn init(reader: Reader, allocator: AllocatorType) Self {
            return .{
                .reader = reader,
                .allocator = allocator,
            };
        }

        pub fn readNil(self: Self) !void {
            try unpackNull(self.reader);
        }

        pub fn readBool(self: Self, comptime T: type) !T {
            return unpackBool(self.reader, T);
        }

        inline fn readIntValue(self: Self, comptime SourceType: type, comptime TargetType: type) !TargetType {
            const size = @divExact(@bitSizeOf(SourceType), 8);
            var buf: [size]u8 = undefined;
            const actual_size = try self.reader.readAll(&buf);
            if (actual_size != size) {
                return error.InvalidFormat;
            }
            const value = std.mem.readInt(SourceType, &buf, .big);

            const source_type_info = @typeInfo(SourceType).Int;
            const target_type_info = @typeInfo(TargetType).Int;

            if (source_type_info.signedness == target_type_info.signedness and source_type_info.bits <= target_type_info.bits) {
                return @intCast(value);
            }
            if (value >= std.math.minInt(TargetType) and value <= std.math.maxInt(TargetType)) {
                return @intCast(value);
            }
            return error.IntegerOverflow;
        }

        pub fn readInt(self: Self, comptime T: type) !T {
            comptime var Type: type = T;
            comptime var type_info: std.builtin.Type = @typeInfo(T);
            comptime var is_optional: bool = false;

            if (type_info == .Optional) {
                Type = type_info.Optional.child;
                type_info = @typeInfo(type_info.Optional.child);
                is_optional = true;
            }

            if (type_info != .Int) {
                @compileError("Expected int type, not '" ++ @typeName(T) ++ "'");
            }

            const byte = try self.reader.readByte();

            if (byte <= MSG_POSITIVE_FIXINT_MAX) {
                return @intCast(byte);
            }

            if (byte >= MSG_NEGATIVE_FIXINT_MIN) {
                const value: i8 = @bitCast(byte);
                if (type_info.Int.signedness == .signed) {
                    return value;
                } else if (value >= 0) {
                    return @intCast(value);
                }
                return error.IntegerOverflow;
            }

            switch (byte) {
                MSG_NIL => return if (is_optional) null else error.InvalidFormat,
                MSG_INT8 => return try self.readIntValue(i8, Type),
                MSG_INT16 => return try self.readIntValue(i16, Type),
                MSG_INT32 => return try self.readIntValue(i32, Type),
                MSG_INT64 => return try self.readIntValue(i64, Type),
                MSG_UINT8 => return try self.readIntValue(u8, Type),
                MSG_UINT16 => return try self.readIntValue(u16, Type),
                MSG_UINT32 => return try self.readIntValue(u32, Type),
                MSG_UINT64 => return try self.readIntValue(u64, Type),
                else => return error.InvalidFormat,
            }
        }

        pub fn readFloat(self: Self, comptime T: type) !T {
            return unpackFloat(self.reader, T);
        }

        pub fn readStringHeader(self: Self, comptime nullable: Nullable) !NullableType(usize, nullable) {
            const byte = try self.reader.readByte();
            switch (byte) {
                MSG_FIXARRAY_MIN...MSG_FIXSTR_MAX => return byte - MSG_FIXSTR_MIN,
                MSG_STR8 => return try self.readIntValue(u8, usize),
                MSG_STR16 => return try self.readIntValue(u16, usize),
                MSG_STR32 => return try self.readIntValue(u32, usize),
                MSG_NIL => return if (nullable == .optional) null else error.InvalidFormat,
                else => return error.InvalidFormat,
            }
        }

        pub fn readString(self: Self, comptime nullable: Nullable) !NullableType([]u8, nullable) {
            if (AllocatorType == NoAllocator) {
                @compileError("No allocator provided");
            }

            const size = if (nullable == .optional)
                try self.readStringHeader(nullable) orelse return null
            else
                try self.readStringHeader(nullable);

            const buf = try self.allocator.alloc(u8, size);
            errdefer self.allocator.free(buf);

            const actual_size = try self.reader.readAll(buf);
            if (actual_size != size) {
                return error.InvalidFormat;
            }

            return buf;
        }

        pub fn readStringInto(self: Self, buffer: []u8, comptime nullable: Nullable) !NullableType([]u8, nullable) {
            const size = if (nullable == .optional)
                try self.readStringHeader(nullable) orelse return null
            else
                try self.readStringHeader(nullable);

            if (buffer.len < size) {
                return error.NoSpaceLeft;
            }

            const buf = buffer[0..size];

            const actual_size = try self.reader.readAll(buf);
            if (actual_size != size) {
                return error.InvalidFormat;
            }

            return buf;
        }

        pub fn readArrayHeader(self: Self, comptime opt: Nullable) !NullableType(usize, opt) {
            const byte = try self.reader.readByte();
            switch (byte) {
                MSG_FIXARRAY_MIN...MSG_FIXARRAY_MAX => return byte - MSG_FIXARRAY_MIN,
                MSG_ARRAY16 => return try self.readIntValue(u16, usize),
                MSG_ARRAY32 => return try self.readIntValue(u32, usize),
                MSG_NIL => return if (opt == .optional) null else error.InvalidFormat,
                else => return error.InvalidFormat,
            }
        }

        pub fn readArray(self: Self, comptime T: type, comptime opt: Nullable) !NullableType([]T, opt) {
            if (AllocatorType == NoAllocator) {
                @compileError("No allocator provided");
            }

            const size = if (opt == .optional)
                try self.readArrayHeader(opt) orelse return null
            else
                try self.readArrayHeader(opt);

            const result = try self.allocator.alloc(T, size);
            errdefer self.allocator.free(result);

            for (result) |*item| {
                item.* = try self.read(T);
            }

            return result;
        }

        pub fn readMapHeader(self: Self, comptime optional: Nullable) !NullableType(u32, optional) {
            const byte = try self.reader.readByte();
            switch (byte) {
                MSG_FIXMAP_MIN...MSG_FIXMAP_MAX => return byte - MSG_FIXMAP_MIN,
                MSG_MAP16 => return try self.readIntValue(u16, u32),
                MSG_MAP32 => return try self.readIntValue(u32, u32),
                MSG_NIL => if (optional == .optional) return null else return error.InvalidFormat,
                else => return error.InvalidFormat,
            }
        }

        pub fn readStruct(self: Self, comptime T: type, comptime optional: Nullable, comptime extra_fields: u16) !NullableType(T, optional) {
            const type_info = @typeInfo(T);
            if (type_info != .Struct) {
                @compileError("Expected struct type");
            }

            const fields = type_info.Struct.fields;
            var result: T = undefined;

            var fields_set = std.bit_set.StaticBitSet(fields.len).initEmpty();

            comptime var max_field_name_len = 0;
            inline for (fields) |field| {
                max_field_name_len = @max(max_field_name_len, field.name.len);
            }
            var field_name_buffer: [max_field_name_len]u8 = undefined;

            const format = if (std.meta.hasFn(T, "msgpackFormat")) T.msgpackFormat() else default_struct_format;
            switch (format) {
                .as_map => |opts| {
                    const size = if (optional == .optional)
                        try self.readMapHeader(.optional) orelse return null
                    else
                        try self.readMapHeader(.required);

                    if (size < extra_fields) return error.InvalidFormat;
                    if (size > fields.len + extra_fields) return error.InvalidFormat;

                    var j: usize = 0;
                    while (j < size - extra_fields) : (j += 1) {
                        switch (opts.key) {
                            .field_index => {
                                const field_no = try self.readInt(u8);
                                inline for (fields, 0..) |field, i| {
                                    if (field_no == i) {
                                        fields_set.set(i);
                                        @field(result, field.name) = try self.read(field.type);
                                        break;
                                    }
                                } else {
                                    return error.InvalidFormat;
                                }
                            },
                            .field_name => {
                                const field_name = try self.readStringInto(&field_name_buffer, .required);
                                inline for (fields, 0..) |field, i| {
                                    if (std.mem.eql(u8, field.name, field_name)) {
                                        fields_set.set(i);
                                        @field(result, field.name) = try self.read(field.type);
                                        break;
                                    }
                                } else {
                                    return error.InvalidFormat;
                                }
                            },
                            .field_name_prefix => {
                                const field_name = try self.readStringInto(&field_name_buffer, .required);
                                inline for (fields, 0..) |field, i| {
                                    if (std.mem.startsWith(u8, field.name, field_name)) {
                                        fields_set.set(i);
                                        @field(result, field.name) = try self.read(field.type);
                                        break;
                                    }
                                } else {
                                    return error.InvalidFormat;
                                }
                            },
                        }
                    }
                },
                .as_array => {
                    const size = if (optional == .optional)
                        try self.readArrayHeader(.optional) orelse return null
                    else
                        try self.readArrayHeader(.required);

                    if (size < extra_fields) return error.InvalidFormat;
                    if (size > fields.len + extra_fields) return error.InvalidFormat;

                    inline for (fields, 0..) |field, i| {
                        fields_set.set(i);
                        @field(result, field.name) = try self.read(field.type);
                    }
                },
            }

            inline for (fields, 0..) |field, i| {
                if (!fields_set.isSet(i)) {
                    if (field.default_value) |default_field_value_ptr| {
                        const default_field_value = @as(*field.type, @ptrCast(@alignCast(@constCast(default_field_value_ptr)))).*;
                        @field(result, field.name) = default_field_value;
                        fields_set.set(i);
                    } else if (@typeInfo(field.type) == .Optional) {
                        @field(result, field.name) = null;
                        fields_set.set(i);
                    }
                }
            }

            if (fields_set.count() != fields.len) return error.InvalidFormat;

            return result;
        }

        pub fn readUnionOrNull(self: Self, comptime T: type) !?T {
            const type_info = @typeInfo(T);
            if (type_info != .Union) {
                @compileError("Expected union type, not " ++ @typeName(T));
            }

            if (type_info.Union.tag_type == null) {
                @compileError("Expected tagged union type, not " + @typeName(T));
            }

            const fields = type_info.Union.fields;

            comptime var max_field_name_len = 0;
            inline for (fields) |field| {
                max_field_name_len = @max(max_field_name_len, field.name.len);
            }
            var field_name_buffer: [max_field_name_len]u8 = undefined;

            var result: T = undefined;

            const format: UnionFormat = if (std.meta.hasFn(T, "msgpackFormat")) T.msgpackFormat() else default_union_format;
            switch (format) {
                .as_map => |opts| {
                    const size = try self.readMapHeader(.optional) orelse return null;
                    if (size != 1) {
                        return error.InvalidFormat;
                    }

                    switch (opts.key) {
                        .field_index => {
                            const field_no = try self.readInt(u8);
                            inline for (fields, 0..) |field, i| {
                                if (field_no == i) {
                                    if (field.type == void) {
                                        try self.readNil();
                                        result = @unionInit(T, field.name, {});
                                    } else {
                                        const value = try self.read(field.type);
                                        result = @unionInit(T, field.name, value);
                                    }
                                    break;
                                }
                            } else {
                                return error.InvalidFormat;
                            }
                        },
                        .field_name => {
                            const field_name = try self.readStringInto(&field_name_buffer, .required);
                            inline for (fields) |field| {
                                if (std.mem.eql(u8, field.name, field_name)) {
                                    if (field.type == void) {
                                        try self.readNil();
                                        result = @unionInit(T, field.name, {});
                                    } else {
                                        const value = try self.read(field.type);
                                        result = @unionInit(T, field.name, value);
                                    }
                                    break;
                                }
                            } else {
                                return error.InvalidFormat;
                            }
                        },
                        .field_name_prefix => {
                            const field_name = try self.readStringInto(&field_name_buffer, .required);
                            inline for (fields) |field| {
                                if (std.mem.startsWith(u8, field.name, field_name)) {
                                    if (field.type == void) {
                                        try self.readNil();
                                        result = @unionInit(T, field.name, {});
                                    } else {
                                        const value = try self.read(field.type);
                                        result = @unionInit(T, field.name, value);
                                    }
                                }
                                break;
                            } else {
                                return error.InvalidFormat;
                            }
                        },
                    }
                },
            }

            return result;
        }

        pub fn readUnion(self: Self, comptime T: type) !T {
            return try self.readUnionOrNull(T) orelse return error.InvalidFormat;
        }

        fn resolveValueType(comptime T: type) type {
            const type_info = @typeInfo(T);
            if (type_info == .Optional) {
                return type_info.Optional.child;
            }
            return T;
        }

        pub fn read(self: Self, comptime T: type) !T {
            const type_info = @typeInfo(T);
            switch (type_info) {
                .Bool => return try self.readBool(T),
                .Int => return try self.readInt(T),
                .Float => return try self.readFloat(T),
                .Pointer => {
                    if (type_info.Pointer.size == .Slice) {
                        return try self.readArray(type_info.Pointer.child, .required);
                    }
                },
                .Union => return try self.readUnion(T),
                .Struct => return try self.readStruct(T, .required, 0),
                .Optional => {
                    const child_type_info = @typeInfo(type_info.Optional.child);
                    switch (child_type_info) {
                        .Bool => return try self.readBool(T),
                        .Int => return try self.readInt(T),
                        .Float => return try self.readFloat(T),
                        .Pointer => {
                            if (type_info.Pointer.size == .Slice) {
                                return try self.readArray(type_info.Pointer.child, .optional, 0);
                            }
                        },
                        .Struct => return try self.readStruct(T, .optional),
                        else => {},
                    }
                },
                else => {},
            }
            @compileError("Unsupported type " ++ @typeName(T));
        }
    };
}

pub fn packer(writer: anytype) Packer(@TypeOf(writer)) {
    return Packer(@TypeOf(writer)).init(writer);
}

pub fn unpacker(reader: anytype, allocator: std.mem.Allocator) Unpacker(@TypeOf(reader), std.mem.Allocator) {
    return Unpacker(@TypeOf(reader), std.mem.Allocator).init(reader, allocator);
}

pub fn unpackerNoAlloc(reader: anytype) Unpacker(@TypeOf(reader), NoAllocator) {
    return Unpacker(@TypeOf(reader), NoAllocator).init(reader, .{});
}

const UnpackOptions = struct {
    allocator: ?std.mem.Allocator = null,
};

pub fn unpack(comptime T: type, reader: anytype, options: UnpackOptions) !T {
    if (options.allocator) |allocator| {
        return try unpacker(reader, allocator).read(T);
    } else {
        return try unpackerNoAlloc(reader).read(T);
    }
}

pub fn unpackFromBytes(comptime T: type, bytes: []const u8, options: UnpackOptions) !T {
    var stream = std.io.fixedBufferStream(bytes);
    return try unpack(T, stream.reader(), options);
}

pub fn pack(comptime T: type, writer: anytype, value: anytype) !void {
    return try packer(writer).write(T, value);
}

fn isArraylist(comptime T: type) bool {
    if (@typeInfo(T) != .Struct or !@hasDecl(T, "Slice"))
        return false;

    const Slice = T.Slice;
    const ptr_info = switch (@typeInfo(Slice)) {
        .pointer => |info| info,
        else => return false,
    };

    return T == std.ArrayListAlignedUnmanaged(ptr_info.child, null) or
        T == std.ArrayListAlignedUnmanaged(ptr_info.child, ptr_info.alignment) or
        T == std.ArrayListAligned(ptr_info.child, null) or
        T == std.ArrayListAligned(ptr_info.child, ptr_info.alignment);
}

test {
    _ = @import("msgpack_test.zig");
}
