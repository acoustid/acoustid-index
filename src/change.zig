const msgpack = @import("msgpack");

pub const Insert = struct {
    id: u32,
    hashes: []const u32,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const Delete = struct {
    id: u32,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const Change = union(enum) {
    insert: Insert,
    delete: Delete,

    pub fn msgpackFormat() msgpack.UnionFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const Metadata = @import("Metadata.zig");

pub const Transaction = struct {
    id: u64,
    changes: []const Change,
    metadata: ?Metadata,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};
