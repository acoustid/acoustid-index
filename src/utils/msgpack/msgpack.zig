const std = @import("std");
const Allocator = std.mem.Allocator;

const MSG_POSITIVE_FIXINT_MIN = 0x00;
const MSG_POSITIVE_FIXINT_MAX = 0x7f;
const MSG_FIXMAP = 0x80;
const MSG_FIXARRAY = 0x90;
const MSG_FIXSTR = 0xa0;
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

pub const StructFormat = enum {
    map_by_name,
    map_by_index,
    array,
};

pub const Options = struct {
    struct_format: StructFormat = .map_by_name,
    omit_nulls: bool = true,
    omit_defaults: bool = false,
};

const NoAllocator = struct {};

pub fn Packer(comptime Writer: type) type {
    return struct {
        writer: Writer,
        options: Options,

        const Self = @This();

        pub fn init(writer: Writer, options: Options) Self {
            return Self{
                .writer = writer,
                .options = options,
            };
        }

        pub fn writeNil(self: *Self) !void {
            try self.writer.writeByte(MSG_NIL);
        }

        pub fn writeBool(self: *Self, value: bool) !void {
            try self.writer.writeByte(if (value) MSG_TRUE else MSG_FALSE);
        }

        fn writeFixedSizeIntValue(self: *Self, comptime T: type, value: T) !void {
            var buf: [@sizeOf(T)]u8 = undefined;
            std.mem.writeInt(T, buf[0..], value, .big);
            try self.writer.writeAll(buf[0..]);
        }

        fn writeFixedSizeInt(self: *Self, comptime T: type, value: T) !void {
            const type_info = @typeInfo(T);
            switch (type_info.Int.signedness) {
                .signed => {
                    switch (type_info.Int.bits) {
                        8 => try self.writer.writeByte(MSG_INT8),
                        16 => try self.writer.writeByte(MSG_INT16),
                        32 => try self.writer.writeByte(MSG_INT32),
                        64 => try self.writer.writeByte(MSG_INT64),
                        else => @compileError("Unsupported signed int with " ++ type_info.Int.bits ++ "bits"),
                    }
                },
                .unsigned => {
                    switch (type_info.Int.bits) {
                        8 => try self.writer.writeByte(MSG_UINT8),
                        16 => try self.writer.writeByte(MSG_UINT16),
                        32 => try self.writer.writeByte(MSG_UINT32),
                        64 => try self.writer.writeByte(MSG_UINT64),
                        else => @compileError("Unsupported unsigned int with " ++ type_info.Int.bits ++ "bits"),
                    }
                },
            }
            try self.writeFixedSizeIntValue(T, value);
        }

        pub fn writeInt(self: *Self, comptime T: type, value: T) !void {
            const type_info = @typeInfo(T);
            if (type_info != .Int) {
                @compileError("Expected integer type");
            }

            const is_signed = type_info.Int.signedness == .signed;
            const bits = type_info.Int.bits;

            if (is_signed) {
                if (value >= -32 and value <= -1) {
                    try self.writer.writeByte(@bitCast(@as(i8, @intCast(value))));
                    return;
                } else if (value >= 0 and value <= 127) {
                    try self.writer.writeByte(@bitCast(@as(u8, @intCast(value))));
                    return;
                }
                if (bits == 8 or value >= std.math.minInt(i8) and value <= std.math.maxInt(i8)) {
                    return self.writeFixedSizeInt(i8, @intCast(value));
                }
                if (bits == 16 or value >= std.math.minInt(i16) and value <= std.math.maxInt(i16)) {
                    return self.writeFixedSizeInt(i16, @intCast(value));
                }
                if (bits == 32 or value >= std.math.minInt(i32) and value <= std.math.maxInt(i32)) {
                    return self.writeFixedSizeInt(i32, @intCast(value));
                }
                if (bits == 64 or value >= std.math.minInt(i64) and value <= std.math.maxInt(i64)) {
                    return self.writeFixedSizeInt(i64, @intCast(value));
                }
                @compileError("Unsupported signed int with " ++ type_info.Int.bits ++ "bits");
            } else {
                if (value <= 127) {
                    return self.writer.writeByte(@bitCast(@as(u8, @intCast(value))));
                }
                if (bits == 8 or value <= std.math.maxInt(u8)) {
                    return self.writeFixedSizeInt(u8, @intCast(value));
                }
                if (bits == 16 or value <= std.math.maxInt(u16)) {
                    return self.writeFixedSizeInt(u16, @intCast(value));
                }
                if (bits == 32 or value <= std.math.maxInt(u32)) {
                    return self.writeFixedSizeInt(u32, @intCast(value));
                }
                if (bits == 64 or value <= std.math.maxInt(u64)) {
                    return self.writeFixedSizeInt(u64, @intCast(value));
                }
                @compileError("Unsupported integer size of " ++ bits ++ "bits");
            }
        }

        pub fn writeFloat(self: *Self, comptime T: type, value: T) !void {
            const type_info = @typeInfo(T);
            if (type_info != .Float) {
                @compileError("Expected float type");
            }

            const bits = type_info.Float.bits;
            switch (bits) {
                32 => try self.writer.writeByte(MSG_FLOAT32),
                64 => try self.writer.writeByte(MSG_FLOAT64),
                else => @compileError("Unsupported float size"),
            }

            var buf: [@sizeOf(T)]u8 = undefined;
            const int = @as(std.meta.Int(.unsigned, @sizeOf(T) * 8), @bitCast(value));
            std.mem.writeInt(@TypeOf(int), buf[0..], int, .big);
            try self.writer.writeAll(buf[0..]);
        }

        pub fn writeStringHeader(self: *Self, len: usize) !void {
            if (len <= 31) {
                try self.writer.writeByte(MSG_FIXSTR | @as(u8, @intCast(len)));
            } else if (len <= std.math.maxInt(u8)) {
                try self.writer.writeByte(MSG_STR8);
                try self.writer.writeByte(@as(u8, @intCast(len)));
            } else if (len <= std.math.maxInt(u16)) {
                try self.writer.writeByte(MSG_STR16);
                try self.writeFixedSizeIntValue(u16, @intCast(len));
            } else if (len <= std.math.maxInt(u32)) {
                try self.writer.writeByte(MSG_STR32);
                try self.writeFixedSizeIntValue(u32, @intCast(len));
            } else {
                return error.StringTooLong;
            }
        }

        pub fn writeString(self: *Self, value: []const u8) !void {
            try self.writeStringHeader(value.len);
            try self.writer.writeAll(value);
        }

        pub fn writeBinaryHeader(self: *Self, len: usize) !void {
            if (len <= std.math.maxInt(u8)) {
                try self.writer.writeByte(MSG_BIN8);
                try self.writer.writeByte(@as(u8, @intCast(len)));
            } else if (len <= std.math.maxInt(u16)) {
                try self.writer.writeByte(MSG_BIN16);
                try self.writeFixedSizeIntValue(u16, @intCast(len));
            } else if (len <= std.math.maxInt(u32)) {
                try self.writer.writeByte(MSG_BIN32);
                try self.writeFixedSizeIntValue(u32, @intCast(len));
            } else {
                return error.BinaryTooLong;
            }
        }

        pub fn writeBinary(self: *Self, value: []const u8) !void {
            try self.writeBinaryHeader(value.len);
            try self.writer.writeAll(value);
        }

        pub fn writeArrayHeader(self: *Self, len: usize) !void {
            if (len <= 15) {
                try self.writer.writeByte(MSG_FIXARRAY | @as(u8, @intCast(len)));
            } else if (len <= std.math.maxInt(u16)) {
                try self.writer.writeByte(MSG_ARRAY16);
                try self.writeFixedSizeIntValue(u16, @intCast(len));
            } else if (len <= std.math.maxInt(u32)) {
                try self.writer.writeByte(MSG_ARRAY32);
                try self.writeFixedSizeIntValue(u32, @intCast(len));
            } else {
                return error.ArrayTooLong;
            }
        }

        pub fn writeArray(self: *Self, comptime T: type, value: []const T) !void {
            try self.writeArrayHeader(value.len);
            for (value) |item| {
                try self.write(T, item);
            }
        }

        pub fn writeMapHeader(self: *Self, len: usize) !void {
            if (len <= 15) {
                try self.writer.writeByte(MSG_FIXMAP | @as(u8, @intCast(len)));
            } else if (len <= std.math.maxInt(u16)) {
                try self.writer.writeByte(MSG_MAP16);
                try self.writeFixedSizeIntValue(u16, @intCast(len));
            } else if (len <= std.math.maxInt(u32)) {
                try self.writer.writeByte(MSG_MAP32);
                try self.writeFixedSizeIntValue(u32, @intCast(len));
            } else {
                return error.MapTooLong;
            }
        }

        fn isStructFieldUsed(self: *Self, field: std.builtin.Type.StructField, value: anytype) bool {
            const field_type_info = @typeInfo(field.type);
            const field_value = @field(value, field.name);

            if (self.options.omit_defaults) {
                if (field.default_value) |default_field_value_ptr| {
                    const default_field_value = @as(*field.type, @ptrCast(@alignCast(@constCast(default_field_value_ptr)))).*;
                    if (field_value == default_field_value) {
                        return false;
                    }
                }
            }

            if (self.options.omit_nulls) {
                if (field_type_info == .Optional) {
                    if (field_value == null) {
                        return false;
                    }
                }
            }

            return true;
        }

        fn countUsedStructFields(self: *Self, fields: []const std.builtin.Type.StructField, value: anytype) u16 {
            var used_field_count: u16 = 0;
            inline for (fields) |field| {
                if (self.isStructFieldUsed(field, value)) {
                    used_field_count += 1;
                }
            }
            return used_field_count;
        }

        pub fn writeStruct(self: *Self, comptime T: type, value: T) !void {
            const type_info = @typeInfo(T);
            if (type_info != .Struct) {
                @compileError("Expected struct type");
            }

            const fields = type_info.Struct.fields;
            if (fields.len > 255) {
                @compileError("Too many fields");
            }

            switch (self.options.struct_format) {
                .map_by_index => {
                    try self.writeMapHeader(self.countUsedStructFields(fields, value));
                    inline for (fields, 0..) |field, i| {
                        if (self.isStructFieldUsed(field, value)) {
                            try self.writeInt(u8, @intCast(i));
                            try self.write(field.type, @field(value, field.name));
                        }
                    }
                },
                .map_by_name => {
                    try self.writeMapHeader(self.countUsedStructFields(fields, value));
                    inline for (fields) |field| {
                        if (self.isStructFieldUsed(field, value)) {
                            try self.writeString(field.name);
                            try self.write(field.type, @field(value, field.name));
                        }
                    }
                },
                .array => {
                    try self.writeArrayHeader(fields.len);
                    inline for (fields) |field| {
                        try self.write(field.type, @field(value, field.name));
                    }
                },
            }
        }

        pub fn writeUnion(self: *Self, comptime T: type, value: T) !void {
            const type_info = @typeInfo(T);
            if (type_info != .Union) {
                @compileError("Expected union type , not " ++ @typeName(T));
            }

            if (type_info.Union.tag_type) |TagType| {
                try self.writeMapHeader(1);
                inline for (type_info.Union.fields, 0..) |field, i| {
                    if (value == @field(TagType, field.name)) {
                        try self.writeInt(u8, @intCast(i));
                        if (field.type == void) {
                            try self.writeNil();
                        } else {
                            try self.write(field.type, @field(value, field.name));
                        }
                        break;
                    }
                } else {
                    unreachable;
                }
            } else {
                @compileError("Unable to write untagged union '" ++ @typeName(T) ++ "'");
            }
        }

        pub fn writePointer(self: *Self, comptime T: type, value: T) !void {
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

        pub fn write(self: *Self, comptime T: type, value: T) !void {
            const type_info = @typeInfo(T);
            switch (type_info) {
                .Bool => try self.writeBool(value),
                .Int => try self.writeInt(T, value),
                .Float => try self.writeFloat(T, value),
                .Optional => {
                    if (value) |val| {
                        try self.write(type_info.Optional.child, val);
                    } else {
                        try self.writeNil();
                    }
                },
                .Array => try self.writeArray(type_info.Array.child, &value),
                .Struct => try self.writeStruct(T, value),
                .Union => try self.writeUnion(T, value),
                .Pointer => try self.writePointer(T, value),
                else => @compileError("Unsupported type " ++ @typeName(T)),
            }
        }
    };
}

pub fn Unpacker(comptime Reader: type, comptime AllocatorType: type, comptime options: Options) type {
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

        pub fn readNil(self: *Self) !void {
            const byte = try self.reader.readByte();
            switch (byte) {
                MSG_NIL => {},
                else => return error.InvalidFormat,
            }
        }

        pub fn readBoolOrNull(self: *Self) !?bool {
            const byte = try self.reader.readByte();
            switch (byte) {
                MSG_NIL => return null,
                MSG_TRUE => return true,
                MSG_FALSE => return false,
                else => return error.InvalidFormat,
            }
        }

        pub fn readBool(self: *Self) !bool {
            return try self.readBoolOrNull() orelse return error.InvalidFormat;
        }

        pub fn readIntValue(self: *Self, comptime SourceInt: type, comptime TargetInt: type) !TargetInt {
            const size = @divExact(@bitSizeOf(SourceInt), 8);
            var buf: [size]u8 = undefined;
            try self.reader.readNoEof(&buf);
            const value = std.mem.readInt(SourceInt, &buf, .big);

            const source_type_info = @typeInfo(SourceInt).Int;
            const target_type_info = @typeInfo(TargetInt).Int;

            if (source_type_info.signedness == target_type_info.signedness and source_type_info.bits <= target_type_info.bits) {
                return @intCast(value);
            }
            if (value >= std.math.minInt(TargetInt) and value <= std.math.maxInt(TargetInt)) {
                return @intCast(value);
            }
            return error.IntegerOverflow;
        }

        pub fn readIntOrNull(self: *Self, comptime T: type) !?T {
            const type_info = @typeInfo(T);
            if (type_info != .Int) {
                @compileError("Expected integer type");
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
                MSG_NIL => return null,
                MSG_INT8 => return try self.readIntValue(i8, T),
                MSG_INT16 => return try self.readIntValue(i16, T),
                MSG_INT32 => return try self.readIntValue(i32, T),
                MSG_INT64 => return try self.readIntValue(i64, T),
                MSG_UINT8 => return try self.readIntValue(u8, T),
                MSG_UINT16 => return try self.readIntValue(u16, T),
                MSG_UINT32 => return try self.readIntValue(u32, T),
                MSG_UINT64 => return try self.readIntValue(u64, T),
                else => return error.InvalidFormat,
            }
        }

        pub fn readInt(self: *Self, comptime T: type) !T {
            return try self.readIntOrNull(T) orelse return error.InvalidFormat;
        }

        pub fn readFloatValue(self: *Self, comptime SourceFloat: type, comptime TargetFloat: type) !TargetFloat {
            const size = @divExact(@bitSizeOf(SourceFloat), 8);
            var buf: [size]u8 = undefined;
            const actual_size = try self.reader.readAll(&buf);
            if (actual_size != size) {
                return error.InvalidFormat;
            }

            const SourceInt = std.meta.Int(.unsigned, @bitSizeOf(SourceFloat));
            const int_value = std.mem.readInt(SourceInt, &buf, .big);

            const value: SourceFloat = @bitCast(int_value);

            return @floatCast(value);
        }

        pub fn readFloatOrNull(self: *Self, comptime T: type) !?T {
            const byte = try self.reader.readByte();
            switch (byte) {
                MSG_NIL => return null,
                MSG_FLOAT32 => return try self.readFloatValue(f32, T),
                MSG_FLOAT64 => return try self.readFloatValue(f64, T),
                else => return error.InvalidFormat,
            }
        }

        pub fn readFloat(self: *Self, comptime T: type) !T {
            return try self.readFloatOrNull(T) orelse return error.InvalidFormat;
        }

        pub fn readString(self: *Self) ![]const u8 {
            if (AllocatorType == NoAllocator) {
                @compileError("No allocator provided");
            }
            const size = try self.readStringHeader();
            const buf = try self.allocator.alloc(u8, size);
            errdefer self.allocator.free(buf);
            try self.reader.readNoEof(buf);
            return buf;
        }

        pub fn readArray(self: *Self, comptime T: type, allocator: std.mem.Allocator) ![]T {
            const byte = try self.reader.readByte();
            const len = switch (byte) {
                0xdc => try self.reader.readIntBig(u16),
                0xdd => try self.reader.readIntBig(u32),
                else => if (byte < 0x90 or byte > 0x9f) {
                    return error.InvalidFormat;
                } else {
                    byte & 0xf;
                },
            };

            const array = try allocator.alloc(T, len);
            errdefer allocator.free(array);
            for (array) |*item| {
                item.* = try self.read(T);
            }
            return array;
        }

        pub fn readStruct(self: *Self, comptime T: type) !T {
            const type_info = @typeInfo(T);
            if (type_info != .Struct) {
                @compileError("Expected struct type");
            }

            const fields = type_info.Struct.fields;
            var result: T = undefined;

            var fields_set = std.bit_set.StaticBitSet(fields.len).initEmpty();

            inline for (fields, 0..) |field, i| {
                if (field.default_value) |default_field_value_ptr| {
                    const default_field_value = @as(*field.type, @ptrCast(@alignCast(@constCast(default_field_value_ptr)))).*;
                    @field(result, field.name) = default_field_value;
                    fields_set.set(i);
                }
                if (@typeInfo(field.type) == .Optional) {
                    @field(result, field.name) = null;
                    fields_set.set(i);
                }
            }

            switch (options.struct_format) {
                .map_by_index => {
                    const size = try self.readMapHeader();
                    if (size > fields.len) return error.InvalidFormat;

                    var j: usize = 0;
                    while (j < size) : (j += 1) {
                        const index = try self.readInt(u8);
                        inline for (fields, 0..) |field, i| {
                            if (index == i) {
                                fields_set.set(i);
                                @field(result, field.name) = try self.read(field.type);
                                break;
                            }
                        } else {
                            return error.InvalidFormat;
                        }
                    }
                },
                .map_by_name => {
                    const size = try self.readMapHeader();
                    if (size > fields.len) return error.InvalidFormat;

                    var j: usize = 0;
                    while (j < size) : (j += 1) {
                        const name = try self.readString();
                        inline for (fields, 0..) |field, i| {
                            if (std.mem.eql(u8, name, field.name)) {
                                fields_set.set(i);
                                @field(result, field.name) = try self.read(field.type);
                                break;
                            }
                        } else {
                            return error.InvalidFormat;
                        }
                    }
                },
                .array => {
                    const size = try self.readArrayHeader();
                    if (size < fields.len) return error.InvalidFormat;

                    inline for (fields, 0..) |field, i| {
                        fields_set.set(i);
                        @field(result, field.name) = try self.read(field.type);
                    }
                },
            }

            if (fields_set.count() != fields.len) return error.InvalidFormat;

            return result;
        }

        fn resolveValueType(comptime T: type) type {
            const type_info = @typeInfo(T);
            if (type_info == .Optional) {
                return type_info.Optional.child;
            }
            return T;
        }

        pub fn read(self: *Self, comptime T: type) !T {
            const type_info = @typeInfo(T);
            switch (type_info) {
                .Bool => return try self.readBool(),
                .Int => return try self.readInt(T),
                .Float => return try self.readFloat(T),
                .Array => return try self.readArray(type_info.Array.child),
                .Struct => return try self.readStruct(T),
                .Optional => {
                    const child_type_info = @typeInfo(type_info.Optional.child);
                    switch (child_type_info) {
                        .Bool => return try self.readBoolOrNull(),
                        .Int => return try self.readIntOrNull(type_info.Optional.child),
                        .Float => return try self.readFloatOrNull(type_info.Optional.child),
                        else => @compileError("Unsupported type " ++ @typeName(T)),
                    }
                },
                else => @compileError("Unsupported type"),
            }
        }

        fn readMapHeader(self: *Self) !usize {
            const byte = try self.reader.readByte();
            switch (byte) {
                MSG_MAP16 => return try self.readIntValue(u16, usize),
                MSG_MAP32 => return try self.readIntValue(u32, usize),
                else => {
                    if (byte & 0xf0 == MSG_FIXMAP) {
                        return byte & 0xf;
                    } else {
                        return error.InvalidFormat;
                    }
                },
            }
        }

        fn readArrayHeader(self: *Self) !usize {
            const byte = try self.reader.readByte();
            switch (byte) {
                MSG_ARRAY16 => return try self.readIntValue(u16, usize),
                MSG_ARRAY32 => return try self.readIntValue(u32, usize),
                else => {
                    if (byte & 0xf0 == MSG_FIXARRAY) {
                        return byte & 0xf;
                    } else {
                        return error.InvalidFormat;
                    }
                },
            }
        }

        fn readStringHeader(self: *Self) !usize {
            const byte = try self.reader.readByte();
            switch (byte) {
                MSG_STR8 => return try self.readIntValue(u8, usize),
                MSG_STR16 => return try self.readIntValue(u16, usize),
                MSG_STR32 => return try self.readIntValue(u32, usize),
                else => {
                    if (byte & 0xe0 == MSG_FIXSTR) {
                        return byte & 0x1f;
                    } else {
                        return error.InvalidFormat;
                    }
                },
            }
        }
    };
}

pub fn packer(writer: anytype, options: Options) Packer(@TypeOf(writer)) {
    return Packer(@TypeOf(writer)).init(writer, options);
}

pub fn unpackerWithAllocator(reader: anytype, allocator: std.mem.Allocator, comptime options: Options) Unpacker(@TypeOf(reader), std.mem.Allocator, options) {
    return Unpacker(@TypeOf(reader), std.mem.Allocator, options).init(reader, allocator);
}

pub fn unpacker(reader: anytype, comptime options: Options) Unpacker(@TypeOf(reader), NoAllocator, options) {
    return Unpacker(@TypeOf(reader), NoAllocator, options).init(reader, .{});
}

test {
    _ = @import("msgpack_test.zig");
}
