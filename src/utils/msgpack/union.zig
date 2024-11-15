const std = @import("std");
const c = @import("common.zig");

const NonOptional = @import("utils.zig").NonOptional;
const Optional = @import("utils.zig").Optional;
const isOptional = @import("utils.zig").isOptional;

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

pub const UnionAsMapOptions = struct {
    key: union(enum) {
        field_name,
        field_name_prefix: u8,
        field_index,
    },
    omit_nulls: bool = true,
    omit_defaults: bool = false,
};

pub const UnionFormat = union(enum) {
    as_map: UnionAsMapOptions,
};

pub const default_union_format = UnionFormat{
    .as_map = .{
        .key = .field_name,
    },
};

fn strPrefix(src: []const u8, len: usize) []const u8 {
    return src[0..@min(src.len, len)];
}

pub fn packUnionAsMap(writer: anytype, comptime T: type, value: T, opts: UnionAsMapOptions) !void {
    const type_info = @typeInfo(T);
    const fields = type_info.Union.fields;

    const TagType = @typeInfo(T).Union.tag_type.?;

    try packMapHeader(writer, 1);

    inline for (fields, 0..) |field, i| {
        if (value == @field(TagType, field.name)) {
            switch (opts.key) {
                .field_index => {
                    try packInt(writer, u16, i);
                },
                .field_name => {
                    try packString(writer, []const u8, field.name);
                },
                .field_name_prefix => |prefix| {
                    try packString(writer, []const u8, strPrefix(field.name, prefix));
                },
            }
            try packAny(writer, field.type, @field(value, field.name));
        }
    }
}

pub fn packUnion(writer: anytype, comptime T: type, value_or_maybe_null: T) !void {
    const value = try maybePackNull(writer, T, value_or_maybe_null) orelse return;
    const Type = @TypeOf(value);
    const type_info = @typeInfo(Type);

    if (type_info != .Union) {
        @compileError("Expected union type");
    }

    const format = if (std.meta.hasFn(Type, "msgpackFormat")) T.msgpackFormat() else default_union_format;
    switch (format) {
        .as_map => |opts| {
            return packUnionAsMap(writer, Type, value, opts);
        },
    }
}

pub fn unpackUnionAsMap(reader: anytype, allocator: std.mem.Allocator, comptime T: type, opts: UnionAsMapOptions) !T {
    const len = if (@typeInfo(T) == .Optional)
        try unpackMapHeader(reader, ?u16) orelse return null
    else
        try unpackMapHeader(reader, u16);

    if (len != 1) {
        return error.InvalidUnionFieldCount;
    }

    const Type = NonOptional(T);
    const type_info = @typeInfo(Type);
    const fields = type_info.Union.fields;

    var field_name_buffer: [256]u8 = undefined;

    var result: Type = undefined;

    switch (opts.key) {
        .field_index => {
            const field_index = try unpackInt(reader, u16);
            inline for (fields, 0..) |field, i| {
                if (field_index == i) {
                    const value = try unpackAny(reader, allocator, field.type);
                    result = @unionInit(Type, field.name, value);
                    break;
                }
            } else {
                return error.UnknownUnionField;
            }
        },
        .field_name => {
            const field_name = try unpackStringInto(reader, []u8, &field_name_buffer);
            inline for (fields) |field| {
                if (std.mem.eql(u8, field.name, field_name)) {
                    const value = try unpackAny(reader, allocator, field.type);
                    result = @unionInit(Type, field.name, value);
                    break;
                }
            } else {
                return error.UnknownUnionField;
            }
        },
        .field_name_prefix => |prefix| {
            const field_name = try unpackStringInto(reader, []u8, &field_name_buffer);
            inline for (fields) |field| {
                if (std.mem.startsWith(u8, field.name, strPrefix(field_name, prefix))) {
                    const value = try unpackAny(reader, allocator, field.type);
                    result = @unionInit(Type, field.name, value);
                    break;
                }
            } else {
                return error.UnknownUnionField;
            }
        },
    }

    return result;
}

pub fn unpackUnion(reader: anytype, allocator: std.mem.Allocator, comptime T: type) !T {
    const Type = NonOptional(T);

    const format = if (std.meta.hasFn(Type, "msgpackFormat")) T.msgpackFormat() else default_union_format;
    switch (format) {
        .as_map => |opts| {
            return try unpackUnionAsMap(reader, allocator, T, opts);
        },
    }
}
