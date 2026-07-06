const msgpack = @import("msgpack");

pub const Metadata = @import("Metadata.zig");

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

// A metadata key/value pair. The set_metadata op carries a plain list of these
// rather than the Metadata map type directly: Change serializes as a tagged union
// (msgpack field-iterates each variant's struct), and Metadata's internal Allocator
// field isn't serializable — a list of flat pairs is.
pub const MetadataEntry = struct {
    key: []const u8,
    value: []const u8,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const SetMetadata = struct {
    entries: []const MetadataEntry,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const Change = union(enum) {
    insert: Insert,
    delete: Delete,
    // Sets index-level metadata (key/value pairs, newest wins). Metadata rides the
    // op stream — one representation for the local oplog and the replication
    // changelog (mirrored), so metadata replicates like any other op instead of
    // being a side field that gets lost when the consumer re-batches.
    set_metadata: SetMetadata,

    pub fn msgpackFormat() msgpack.UnionFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const Transaction = struct {
    id: u64,
    changes: []const Change,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};
