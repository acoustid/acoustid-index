const std = @import("std");
const msgpack = @import("msgpack");
const Change = @import("change.zig").Change;
const Metadata = @import("Metadata.zig");

// Default values for search requests
pub const default_search_timeout = 500;
pub const max_search_timeout = 10000;
pub const default_search_limit = 40;
pub const min_search_limit = 1;
pub const max_search_limit = 100;

// Request models
pub const SearchRequest = struct {
    query: []u32,
    timeout: u32 = default_search_timeout,
    limit: u32 = default_search_limit,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const UpdateRequest = struct {
    changes: []const Change,
    metadata: ?Metadata = null,
    expected_version: ?u64 = null,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const CreateIndexRequest = struct {
    expect_does_not_exist: bool = false,
    generation: ?u64 = null,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const DeleteIndexRequest = struct {
    expect_exists: bool = false,
    generation: ?u64 = null,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

// Response models
pub const SearchResult = struct {
    id: u32,
    score: u32,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const SearchResponse = struct {
    results: []SearchResult,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const UpdateResponse = struct {
    version: u64,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

// Index info models
pub const IndexStats = struct {
    min_doc_id: u32,
    max_doc_id: u32,
    num_segments: usize,
    num_docs: u32,
};

pub const GetIndexInfoResponse = struct {
    version: u64,
    metadata: Metadata,
    stats: IndexStats,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const CreateIndexResponse = struct {
    version: u64,
    ready: bool,
    generation: u64,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const GetFingerprintInfoResponse = struct {
    version: u64,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};
