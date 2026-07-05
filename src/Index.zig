// A single index. For now it holds only in-memory segments; file segments,
// persistence (via the Log), and merging join this same structure in later
// steps without changing the update/search API.

const std = @import("std");
const zio = @import("zio");
const Change = @import("change.zig").Change;
const Metadata = @import("Metadata.zig");
const MemorySegment = @import("MemorySegment.zig");
const SearchResults = @import("common.zig").SearchResults;

const Self = @This();

pub const UpdateOptions = struct {
    // Optimistic concurrency: fail if the current version isn't this.
    expected_version: ?u64 = null,
};

allocator: std.mem.Allocator,

// Guards the segment list and version. Reads take it shared; the single writer
// takes it exclusive. A later RCU/IndexReader snapshot can replace the read-side
// lock without changing this API.
lock: zio.RwLock = .init,

// The version is the log position of the last applied change (log-position-as-
// version). In-memory-only for now, so it's just a monotonic counter.
version: u64 = 0,

// Newest segment last. File segments will join this structure later.
memory_segments: std.ArrayListUnmanaged(*MemorySegment) = .empty,

pub fn init(allocator: std.mem.Allocator) Self {
    return .{ .allocator = allocator };
}

pub fn deinit(self: *Self) void {
    for (self.memory_segments.items) |seg| {
        seg.deinit(.delete);
        self.allocator.destroy(seg);
    }
    self.memory_segments.deinit(self.allocator);
}

// Writer path (runs on the background runtime once the Log is wired in). Each
// update becomes a new memory segment tagged with the new version.
pub fn update(self: *Self, changes: []const Change, metadata: ?Metadata, options: UpdateOptions) !u64 {
    try self.lock.lock();
    defer self.lock.unlock();

    if (options.expected_version) |expected| {
        if (self.version != expected) return error.VersionMismatch;
    }

    const new_version = self.version + 1;

    const seg = try self.allocator.create(MemorySegment);
    errdefer self.allocator.destroy(seg);
    seg.* = MemorySegment.init(self.allocator, .{});
    errdefer seg.deinit(.delete);
    seg.info = .{ .version = new_version, .merges = 0 };

    try seg.build(changes, metadata);
    try self.memory_segments.append(self.allocator, seg);

    self.version = new_version;
    return new_version;
}

// Reader path (runs on the search/API runtime). `hashes` is sorted in place.
pub fn search(self: *Self, hashes: []u32, results: *SearchResults) !void {
    try self.lock.lockShared();
    defer self.lock.unlockShared();

    std.sort.pdq(u32, hashes, {}, std.sort.asc(u32));
    for (self.memory_segments.items) |seg| {
        try seg.search(hashes, results);
    }
    try results.finish(self);
}

// Used by SearchResults.finish to drop hits shadowed by a newer version.
// Called under the shared lock held by search(), so it must not re-lock.
pub fn hasNewerVersion(self: *const Self, id: u32, version: u64) bool {
    var i = self.memory_segments.items.len;
    while (i > 0) {
        i -= 1;
        const seg = self.memory_segments.items[i];
        if (seg.info.version > version) {
            if (id >= seg.min_doc_id and id <= seg.max_doc_id and seg.docs.contains(id)) {
                return true;
            }
        } else break;
    }
    return false;
}
