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

// One durable commit in the oplog. `id` is the internal commit id (dense, minted
// locally); `version` is the upstream changelog position it corresponds to,
// which is what a restarted node resumes the feed from. In standalone mode there is
// no upstream and the two are equal. See SegmentInfo for why they are separate.
pub const Transaction = struct {
    id: u64,
    version: u64 = 0,
    // Whether `version` came from an upstream feed rather than being minted locally.
    // Replay uses it to restore the oplog's sticky external_versions flag, so the rule
    // holds across a restart that happens before the next checkpoint.
    external: bool = false,
    changes: []const Change,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};
