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

var dummy: u8 = 0;

const NoAllocator = struct {
    pub fn noAlloc(ctx: *anyopaque, len: usize, ptr_align: u8, ret_addr: usize) ?[*]u8 {
        _ = ctx;
        _ = len;
        _ = ptr_align;
        _ = ret_addr;
        return null;
    }

    pub fn allocator() std.mem.Allocator {
        return .{
            .ptr = &dummy,
            .vtable = &.{
                .alloc = noAlloc,
                .resize = std.mem.Allocator.noResize,
                .free = std.mem.Allocator.noFree,
            },
        };
    }
};

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

pub const sizeOfPackedString = @import("string.zig").sizeOfPackedString;
pub const sizeOfPackedStringHeader = @import("string.zig").sizeOfPackedStringHeader;
pub const packString = @import("string.zig").packString;
pub const packStringHeader = @import("string.zig").packStringHeader;

pub const sizeOfPackedArray = @import("array.zig").sizeOfPackedArray;
pub const sizeOfPackedArrayHeader = @import("array.zig").sizeOfPackedArrayHeader;
pub const packArray = @import("array.zig").packArray;
pub const packArrayHeader = @import("array.zig").packArrayHeader;

pub const sizeOfPackedMap = @import("map.zig").sizeOfPackedMap;
pub const sizeOfPackedMapHeader = @import("map.zig").sizeOfPackedMapHeader;
pub const packMap = @import("map.zig").packMap;
pub const packMapHeader = @import("map.zig").packMapHeader;
pub const unpackMapHeader = @import("map.zig").unpackMapHeader;

pub const unpackString = @import("string.zig").unpackString;
pub const unpackStringInto = @import("string.zig").unpackStringInto;

pub const unpackBinary = @import("binary.zig").unpackBinary;
pub const unpackBinaryInto = @import("binary.zig").unpackBinaryInto;

pub const unpackArrayHeader = @import("array.zig").unpackArrayHeader;

pub const StructFormat = @import("struct.zig").StructFormat;
pub const StructAsMapOptions = @import("struct.zig").StructAsMapOptions;
pub const StructAsArrayOptions = @import("struct.zig").StructAsArrayOptions;
pub const packStruct = @import("struct.zig").packStruct;
pub const unpackStruct = @import("struct.zig").unpackStruct;

pub const UnionFormat = @import("union.zig").UnionFormat;
pub const UnionAsMapOptions = @import("union.zig").UnionAsMapOptions;
pub const packUnion = @import("union.zig").packUnion;
pub const unpackUnion = @import("union.zig").unpackUnion;

pub const packAny = @import("any.zig").packAny;
pub const unpackAny = @import("any.zig").unpackAny;

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

        pub fn writeStringHeader(self: Self, len: usize) !void {
            return packStringHeader(self.writer, len);
        }

        pub fn writeString(self: Self, value: []const u8) !void {
            return packString(self.writer, @TypeOf(value), value);
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
            return sizeOfPackedArrayHeader(len);
        }

        pub fn writeArrayHeader(self: Self, len: usize) !void {
            return packArrayHeader(self.writer, len);
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

        pub fn getMapHeaderSize(len: usize) !usize {
            return sizeOfPackedMapHeader(len);
        }

        pub fn writeMapHeader(self: Self, len: usize) !void {
            return packMapHeader(self.writer, len);
        }

        pub fn writeMap(self: Self, value: anytype) !void {
            return packMap(self.writer, value);
        }

        pub fn writeStruct(self: Self, comptime T: type, value: T) !void {
            return packStruct(self.writer, T, value);
        }

        pub fn writeUnion(self: Self, comptime T: type, value: T) !void {
            return packUnion(self.writer, T, value);
        }

        pub fn write(self: Self, comptime T: type, value: T) !void {
            return packAny(self.writer, T, value);
        }
    };
}

pub fn Unpacker(comptime Reader: type) type {
    return struct {
        reader: Reader,
        allocator: std.mem.Allocator,

        const Self = @This();

        pub fn init(reader: Reader, allocator: std.mem.Allocator) Self {
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

        pub fn readInt(self: Self, comptime T: type) !T {
            return unpackInt(self.reader, T);
        }

        pub fn readFloat(self: Self, comptime T: type) !T {
            return unpackFloat(self.reader, T);
        }

        pub fn readString(self: Self) ![]const u8 {
            return unpackString(self.reader, self.allocator);
        }

        pub fn readStringInto(self: Self, buffer: []u8) ![]const u8 {
            return unpackStringInto(self.reader, buffer);
        }

        pub fn readBinary(self: Self) ![]const u8 {
            return unpackString(self.reader, self.allocator);
        }

        pub fn readBinaryInto(self: Self, buffer: []u8) ![]const u8 {
            return unpackBinaryInto(self.reader, buffer);
        }

        pub fn readArrayHeader(self: Self, comptime opt: Nullable) !NullableType(u32, opt) {
            switch (opt) {
                .optional => return try unpackArrayHeader(self.reader, ?u32),
                .required => return try unpackArrayHeader(self.reader, u32),
            }
        }

        pub fn readMapHeader(self: Self, comptime opt: Nullable) !NullableType(u32, opt) {
            switch (opt) {
                .optional => return try unpackMapHeader(self.reader, ?u32),
                .required => return try unpackMapHeader(self.reader, u32),
            }
        }

        pub fn readArray(self: Self, comptime T: type, comptime opt: Nullable) !NullableType([]T, opt) {
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

        pub fn readStruct(self: Self, comptime T: type) !T {
            return unpackStruct(self.reader, self.allocator, T);
        }

        pub fn readUnion(self: Self, comptime T: type) !?T {
            return unpackUnion(self.reader, self.allocator, T);
        }

        fn resolveValueType(comptime T: type) type {
            const type_info = @typeInfo(T);
            if (type_info == .Optional) {
                return type_info.Optional.child;
            }
            return T;
        }

        pub fn read(self: Self, comptime T: type) !T {
            return unpackAny(self.reader, self.allocator, T);
        }
    };
}

pub fn packer(writer: anytype) Packer(@TypeOf(writer)) {
    return Packer(@TypeOf(writer)).init(writer);
}

pub fn unpacker(reader: anytype, allocator: std.mem.Allocator) Unpacker(@TypeOf(reader)) {
    return Unpacker(@TypeOf(reader)).init(reader, allocator);
}

pub fn unpackerNoAlloc(reader: anytype) Unpacker(@TypeOf(reader)) {
    return Unpacker(@TypeOf(reader)).init(reader, NoAllocator.allocator());
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
    _ = std.testing.refAllDecls(@This());
}
