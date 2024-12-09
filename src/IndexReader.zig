const std = @import("std");

const Self = @This();

const metrics = @import("metrics.zig");
const Deadline = @import("utils/Deadline.zig");
const SearchResults = @import("common.zig").SearchResults;
const SharedPtr = @import("utils/shared_ptr.zig").SharedPtr;
const DocInfo = @import("common.zig").DocInfo;

const SegmentList = @import("segment_list.zig").SegmentList;

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

pub fn search(self: *Self, hashes: []const u32, allocator: std.mem.Allocator, deadline: Deadline) !SearchResults {
    const sorted_hashes = try allocator.dupe(u32, hashes);
    defer allocator.free(sorted_hashes);
    std.sort.pdq(u32, sorted_hashes, {}, std.sort.asc(u32));

    var results = SearchResults.init(allocator);
    errdefer results.deinit();

    inline for (segment_lists) |n| {
        const segments = @field(self, n);
        try segments.value.search(sorted_hashes, &results, deadline);
    }

    results.sort();

    if (results.count() == 0) {
        metrics.searchMiss();
    } else {
        metrics.searchHit();
    }

    return results;
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

pub fn getAttributes(self: *Self, allocator: std.mem.Allocator) !std.StringHashMapUnmanaged(u64) {
    var attributes: std.StringHashMapUnmanaged(u64) = .{};
    errdefer attributes.deinit(allocator);

    inline for (segment_lists) |n| {
        const segments = @field(self, n);
        for (segments.value.nodes.items) |node| {
            var iter = node.value.attributes.iterator();
            while (iter.next()) |entry| {
                try attributes.put(allocator, entry.key_ptr.*, entry.value_ptr.*);
            }
        }
    }

    // builtin attributes
    try attributes.put(allocator, "min_document_id", self.getMinDocId());
    try attributes.put(allocator, "max_document_id", self.getMaxDocId());

    return attributes;
}
