// A single index: an on-disk oplog (WAL) plus in-memory segments. On open the
// oplog is replayed to rebuild the segments. File segments and merging join
// this structure in later steps without changing the update/search API.

const std = @import("std");
const zio = @import("zio");
const Change = @import("change.zig").Change;
const Metadata = @import("Metadata.zig");
const Transaction = @import("change.zig").Transaction;
const MemorySegment = @import("MemorySegment.zig");
const Oplog = @import("Oplog.zig");
const SearchResults = @import("common.zig").SearchResults;

const Self = @This();

allocator: std.mem.Allocator,
dir: zio.Dir,
oplog: Oplog,

// Reads take this shared, the single writer takes it exclusive. A later
// RCU/IndexReader snapshot can replace the read-side lock without changing the
// API. Different indexes have independent locks, so their writes don't contend.
lock: zio.RwLock = .init,

// version = oplog commit id of the last applied change (log-position-as-version).
version: u64 = 0,

// Newest segment last. File segments will join this structure later.
memory_segments: std.ArrayListUnmanaged(*MemorySegment) = .empty,

/// Open the index in `dir` (taking ownership of it): open the oplog and replay
/// it to rebuild the in-memory segments.
pub fn open(allocator: std.mem.Allocator, dir: zio.Dir) !Self {
    var self = Self{ .allocator = allocator, .dir = dir, .oplog = undefined };
    errdefer self.freeSegments();
    self.oplog = try Oplog.open(allocator, dir, &self, applyReplay);
    self.version = self.oplog.last_version;
    return self;
}

fn applyReplay(self: *Self, txn: Transaction) !void {
    try self.memory_segments.ensureUnusedCapacity(self.allocator, 1);
    const seg = try self.buildSegment(txn.changes, txn.metadata);
    self.commitSegment(seg, txn.id);
}

pub fn deinit(self: *Self) void {
    self.oplog.deinit();
    self.freeSegments();
    self.dir.close();
}

fn freeSegments(self: *Self) void {
    for (self.memory_segments.items) |seg| {
        seg.deinit(.delete);
        self.allocator.destroy(seg);
    }
    self.memory_segments.deinit(self.allocator);
}

// Writer path. Build the segment before the durable append so a build failure
// never leaves the log ahead of memory; the oplog append is the commit point.
pub fn update(self: *Self, changes: []const Change, metadata: ?Metadata, expected_version: ?u64) !u64 {
    try self.lock.lock();
    defer self.lock.unlock();

    try self.memory_segments.ensureUnusedCapacity(self.allocator, 1);
    const seg = try self.buildSegment(changes, metadata);
    errdefer {
        seg.deinit(.delete);
        self.allocator.destroy(seg);
    }

    const version = try self.oplog.append(changes, metadata, expected_version);
    self.commitSegment(seg, version);
    return version;
}

fn buildSegment(self: *Self, changes: []const Change, metadata: ?Metadata) !*MemorySegment {
    const seg = try self.allocator.create(MemorySegment);
    errdefer self.allocator.destroy(seg);
    seg.* = MemorySegment.init(self.allocator, .{});
    errdefer seg.deinit(.delete);
    try seg.build(changes, metadata);
    return seg;
}

// Infallible: the caller must have reserved capacity in memory_segments.
fn commitSegment(self: *Self, seg: *MemorySegment, version: u64) void {
    seg.info = .{ .version = version, .merges = 0 };
    self.memory_segments.appendAssumeCapacity(seg);
    self.version = version;
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
