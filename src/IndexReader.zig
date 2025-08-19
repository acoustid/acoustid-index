const std = @import("std");

const Self = @This();

const metrics = @import("metrics.zig");
const Deadline = @import("utils/Deadline.zig");
const SearchResults = @import("common.zig").SearchResults;
const SearchOptions = @import("common.zig").SearchOptions;
const SharedPtr = @import("utils/shared_ptr.zig").SharedPtr;
const DocInfo = @import("common.zig").DocInfo;

const SegmentList = @import("segment_list.zig").SegmentList;
const Metadata = @import("Metadata.zig");

const FileSegment = @import("FileSegment.zig");
const FileSegmentList = SegmentList(FileSegment);

const MemorySegment = @import("MemorySegment.zig");
const MemorySegmentList = SegmentList(MemorySegment);

const segment_lists = [_][]const u8{
    "file_segments",
    "memory_segments",
};

file_segments: SharedPtr(FileSegmentList),
memory_segments: SharedPtr(MemorySegmentList),

pub fn hasNewerVersion(self: *const Self, doc_id: u32, version: u64) bool {
    inline for (segment_lists) |n| {
        const segments = @field(self, n);
        if (segments.value.hasNewerVersion(doc_id, version)) {
            return true;
        }
    }
    return false;
}

pub fn search(self: *Self, hashes: []u32, results: *SearchResults, deadline: Deadline) !void {
    std.sort.pdq(u32, hashes, {}, std.sort.asc(u32));

    inline for (segment_lists) |n| {
        const segments = @field(self, n);
        try segments.value.search(hashes, results, deadline);
    }

    try results.finish(self);
}

pub fn getNumDocs(self: *Self) u32 {
    var result: u32 = 0;
    inline for (segment_lists) |n| {
        const segments = @field(self, n);
        result += segments.value.getNumDocs();
    }
    return result;
}

pub fn getDocInfo(self: *Self, doc_id: u32) !?DocInfo {
    // TODO optimize, read from the end
    var result: ?DocInfo = null;
    inline for (segment_lists) |n| {
        const segments = @field(self, n);
        if (segments.value.getDocInfo(doc_id)) |res| {
            result = res;
        }
    }
    if (result) |res| {
        if (!res.deleted) {
            return res;
        }
    }
    return null;
}

pub fn getMinDocId(self: *Self) u32 {
    var result: u32 = 0;
    inline for (segment_lists) |n| {
        const segments = @field(self, n);
        const doc_id = segments.value.getMinDocId();
        if (result == 0 or doc_id < result) {
            result = doc_id;
        }
    }
    return result;
}

pub fn getMaxDocId(self: *Self) u32 {
    var result: u32 = 0;
    inline for (segment_lists) |n| {
        const segments = @field(self, n);
        const doc_id = segments.value.getMaxDocId();
        if (result == 0 or doc_id > result) {
            result = doc_id;
        }
    }
    return result;
}

pub fn getVersion(self: *Self) u64 {
    if (self.memory_segments.value.getLast()) |node| {
        return node.value.info.version;
    }
    if (self.file_segments.value.getLast()) |node| {
        return node.value.info.version;
    }
    return 0;
}

pub fn getNumSegments(self: *Self) usize {
    return self.memory_segments.value.count() + self.file_segments.value.count();
}

pub fn getMetadata(self: *Self, allocator: std.mem.Allocator) !std.StringHashMapUnmanaged([]const u8) {
    var metadata: std.StringHashMapUnmanaged([]const u8) = .{};
    errdefer metadata.deinit(allocator);

    inline for (segment_lists) |n| {
        const segments = @field(self, n);
        for (segments.value.nodes.items) |node| {
            var iter = node.value.metadata.iterator();
            while (iter.next()) |entry| {
                const result = try metadata.getOrPut(allocator, entry.key);
                if (!result.found_existing) {
                    result.key_ptr.* = entry.key;
                }
                result.value_ptr.* = entry.value;
            }
        }
    }

    return metadata;
}

pub fn getMetadataWrapper(self: *Self, allocator: std.mem.Allocator) !Metadata {
    var metadata = Metadata.initBorrowed(allocator);
    errdefer metadata.deinit();

    inline for (segment_lists) |n| {
        const segments = @field(self, n);
        for (segments.value.nodes.items) |node| {
            var iter = node.value.metadata.iterator();
            while (iter.next()) |entry| {
                if (metadata.get(entry.key) == null) {
                    try metadata.set(entry.key, entry.value);
                }
            }
        }
    }

    return metadata;
}

pub const Stats = struct {
    min_doc_id: u32,
    max_doc_id: u32,
    num_segments: usize,
    num_docs: u32,

    pub fn msgpackWrite(self: Stats, packer: anytype) !void {
        try packer.writeMapHeader(4);
        try packer.write("min");
        try packer.write(self.min_doc_id);
        try packer.write("max");
        try packer.write(self.max_doc_id);
        try packer.write("s");
        try packer.write(self.num_segments);
        try packer.write("d");
        try packer.write(self.num_docs);
    }
};

pub fn getStats(self: *Self) Stats {
    return Stats{
        .min_doc_id = self.getMinDocId(),
        .max_doc_id = self.getMaxDocId(),
        .num_segments = self.getNumSegments(),
        .num_docs = self.getNumDocs(),
    };
}
