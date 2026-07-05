// A single index: an on-disk oplog (WAL) plus in-memory and on-disk (file)
// segments. On open, file segments are loaded from the manifest and the oplog
// tail is replayed. When memory grows past a threshold, all memory segments are
// merged into one file segment (checkpoint); file segments are in turn merged by
// a tiered policy to keep their count bounded.

const std = @import("std");
const zio = @import("zio");
const Change = @import("change.zig").Change;
const Metadata = @import("Metadata.zig");
const Transaction = @import("change.zig").Transaction;
const MemorySegment = @import("MemorySegment.zig");
const FileSegment = @import("FileSegment.zig");
const Oplog = @import("Oplog.zig");
const filefmt = @import("filefmt.zig");
const manifest = @import("manifest.zig");
const SegmentInfo = @import("segment.zig").SegmentInfo;
const SegmentMerger = @import("segment_merger.zig").SegmentMerger;
const TieredMergePolicy = @import("segment_merge_policy.zig").TieredMergePolicy;
const SearchResults = @import("common.zig").SearchResults;
const log = std.log.scoped(.index);

const Self = @This();

allocator: std.mem.Allocator,
dir: zio.Dir,
oplog: Oplog,

// Reads take this shared, the single writer takes it exclusive. Different
// indexes have independent locks, so their writes don't contend.
lock: zio.RwLock = .init,

// version = commit id of the last applied change (log-position-as-version).
version: u64 = 0,

// Highest commit id already captured in a file segment. Oplog replay skips
// transactions at or below this so file + memory segments don't double-count.
file_version: u64 = 0,

// Checkpoint the oldest memory segment once memory exceeds this many items.
checkpoint_threshold: usize = 100_000,

// Both lists are ordered oldest -> newest by version; file segments are older
// than all memory segments.
file_segments: std.ArrayListUnmanaged(*FileSegment) = .empty,
memory_segments: std.ArrayListUnmanaged(*MemorySegment) = .empty,

pub fn open(allocator: std.mem.Allocator, dir: zio.Dir, checkpoint_threshold: usize) !Self {
    var self = Self{ .allocator = allocator, .dir = dir, .oplog = undefined, .checkpoint_threshold = checkpoint_threshold };
    errdefer self.freeSegments();

    // 1. Load file segments listed in the manifest.
    const infos = try manifest.read(dir, allocator);
    defer allocator.free(infos);
    for (infos) |info| {
        const seg = try allocator.create(FileSegment);
        seg.* = FileSegment.init(allocator);
        errdefer {
            seg.deinit(.keep);
            allocator.destroy(seg);
        }
        try filefmt.readSegment(dir, info, seg);
        try self.file_segments.append(allocator, seg);
        self.file_version = @max(self.file_version, info.getLastCommitId());
    }
    self.version = self.file_version;

    // 2. Open the oplog and replay only the tail (versions > file_version).
    self.oplog = try Oplog.open(allocator, dir, &self, applyReplay);
    self.version = @max(self.version, self.oplog.last_version);
    return self;
}

fn applyReplay(self: *Self, txn: Transaction) !void {
    if (txn.id <= self.file_version) return; // already in a file segment
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
    for (self.file_segments.items) |seg| {
        seg.deinit(.keep);
        self.allocator.destroy(seg);
    }
    self.file_segments.deinit(self.allocator);
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

    self.maybeCheckpoint() catch |err| {
        // A checkpoint failure doesn't lose data (it's still in the oplog and
        // memory); just log and let a later update retry.
        log.warn("checkpoint failed: {}", .{err});
    };
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

fn memoryItemCount(self: *const Self) usize {
    var total: usize = 0;
    for (self.memory_segments.items) |seg| total += seg.getSize();
    return total;
}

fn maybeCheckpoint(self: *Self) !void {
    if (self.memory_segments.items.len == 0 or self.memoryItemCount() <= self.checkpoint_threshold) return;
    try self.checkpoint();
    try self.maybeMergeFiles();
}

// Merge all memory segments into a single file segment, make it official in the
// manifest, then clear memory. Crash-safe: the data stays in the oplog until the
// manifest write commits.
fn checkpoint(self: *Self) !void {
    if (self.memory_segments.items.len == 0) return;

    const fseg = try self.mergeToFileSegment(MemorySegment, self.memory_segments.items);
    var committed = false;
    errdefer if (!committed) self.discardFileSegment(fseg);

    try self.file_segments.append(self.allocator, fseg);
    errdefer _ = self.file_segments.pop();

    try self.writeManifest();
    committed = true;

    self.file_version = @max(self.file_version, fseg.info.getLastCommitId());
    for (self.memory_segments.items) |seg| {
        seg.deinit(.delete);
        self.allocator.destroy(seg);
    }
    self.memory_segments.clearRetainingCapacity();

    log.info("checkpointed to file segment {x}-{x} ({} items)", .{ fseg.info.version, fseg.info.merges, fseg.num_items });
}

fn fileSegmentSize(seg: *FileSegment) usize {
    return seg.getSize();
}

fn fileSegmentFrozen(seg: *FileSegment) bool {
    _ = seg;
    return false;
}

fn maybeMergeFiles(self: *Self) !void {
    const policy = TieredMergePolicy(*FileSegment, fileSegmentSize, fileSegmentFrozen){
        .min_segment_size = 100,
        .max_segment_size = 1_000_000_000,
        .segments_per_merge = 10,
        .segments_per_level = 10,
    };
    const budget = policy.calculateBudget(self.file_segments.items);
    if (self.file_segments.items.len <= budget) return;

    const candidate = policy.findSegmentsToMerge(self.file_segments.items) orelse return;
    if (candidate.end - candidate.start < 2) return;
    try self.mergeFileRange(candidate.start, candidate.end);
}

// Merge file_segments[start..end] into one, commit via the manifest, then swap
// the in-memory list and delete the old files.
fn mergeFileRange(self: *Self, start: usize, end: usize) !void {
    const n = end - start;

    const fseg = try self.mergeToFileSegment(FileSegment, self.file_segments.items[start..end]);
    var committed = false;
    errdefer if (!committed) self.discardFileSegment(fseg);

    // Keep the old segment pointers for post-commit cleanup (dupe before the
    // manifest write so an alloc failure stays before the commit point).
    const old_segs = try self.allocator.dupe(*FileSegment, self.file_segments.items[start..end]);
    defer self.allocator.free(old_segs);

    var new_infos = try std.ArrayListUnmanaged(SegmentInfo).initCapacity(self.allocator, self.file_segments.items.len - n + 1);
    defer new_infos.deinit(self.allocator);
    for (self.file_segments.items[0..start]) |s| new_infos.appendAssumeCapacity(s.info);
    new_infos.appendAssumeCapacity(fseg.info);
    for (self.file_segments.items[end..]) |s| new_infos.appendAssumeCapacity(s.info);
    try manifest.write(self.dir, self.allocator, new_infos.items);
    committed = true;

    // Infallible in-memory swap: replace [start..end] with fseg.
    self.file_segments.items[start] = fseg;
    var k = end - 1;
    while (k > start) : (k -= 1) _ = self.file_segments.orderedRemove(k);

    for (old_segs) |s| {
        filefmt.deleteSegmentFile(self.dir, s.info) catch |err| {
            log.warn("failed to delete merged segment {x}: {}", .{ s.info.version, err });
        };
        s.deinit(.keep);
        self.allocator.destroy(s);
    }

    log.info("merged {} file segments -> {x}-{x} ({} items)", .{ n, fseg.info.version, fseg.info.merges, fseg.num_items });
}

// Merge `sources` (all of type Segment) into a new on-disk file segment and load
// it back. On error the written file is cleaned up. Caller owns the result.
fn mergeToFileSegment(self: *Self, comptime Segment: type, sources: []const *Segment) !*FileSegment {
    var merger = try SegmentMerger(Segment).init(self.allocator, sources.len);
    defer merger.deinit();
    for (sources) |s| merger.addSource(s);
    try merger.prepare(self);
    const info = merger.segment.info;

    try filefmt.writeSegment(self.dir, &merger, self.allocator);
    errdefer filefmt.deleteSegmentFile(self.dir, info) catch {};

    const fseg = try self.allocator.create(FileSegment);
    errdefer self.allocator.destroy(fseg);
    fseg.* = FileSegment.init(self.allocator);
    errdefer fseg.deinit(.keep);
    try filefmt.readSegment(self.dir, info, fseg);
    return fseg;
}

fn discardFileSegment(self: *Self, fseg: *FileSegment) void {
    filefmt.deleteSegmentFile(self.dir, fseg.info) catch {};
    fseg.deinit(.keep);
    self.allocator.destroy(fseg);
}

fn writeManifest(self: *Self) !void {
    const infos = try self.allocator.alloc(SegmentInfo, self.file_segments.items.len);
    defer self.allocator.free(infos);
    for (self.file_segments.items, 0..) |seg, i| infos[i] = seg.info;
    try manifest.write(self.dir, self.allocator, infos);
}

// Reader path (runs on the search/API runtime). `hashes` is sorted in place.
pub fn search(self: *Self, hashes: []u32, results: *SearchResults) !void {
    try self.lock.lockShared();
    defer self.lock.unlockShared();

    std.sort.pdq(u32, hashes, {}, std.sort.asc(u32));
    for (self.file_segments.items) |seg| {
        try seg.search(hashes, results);
    }
    for (self.memory_segments.items) |seg| {
        try seg.search(hashes, results);
    }
    try results.finish(self);
}

// Used by SearchResults.finish to drop hits shadowed by a newer version.
// Called under the shared lock held by search(), so it must not re-lock.
// Segments are non-merged (one version each), so info.version is the doc's
// version. Iterate newest -> oldest across both lists (globally descending).
pub fn hasNewerVersion(self: *const Self, id: u32, version: u64) bool {
    var i = self.memory_segments.items.len;
    while (i > 0) {
        i -= 1;
        const seg = self.memory_segments.items[i];
        if (seg.info.version <= version) return false;
        if (id >= seg.min_doc_id and id <= seg.max_doc_id and seg.docs.contains(id)) return true;
    }
    var j = self.file_segments.items.len;
    while (j > 0) {
        j -= 1;
        const seg = self.file_segments.items[j];
        if (seg.info.version <= version) return false;
        if (id >= seg.min_doc_id and id <= seg.max_doc_id and seg.docs.contains(id)) return true;
    }
    return false;
}

fn cleanupTestDir(cwd: zio.Dir, path: []const u8) void {
    var sub = cwd.openDir(path, .{ .iterate = true }) catch return;
    var names: [64][filefmt.max_file_name_size]u8 = undefined;
    var count: usize = 0;
    var it = sub.iterate();
    while (it.next() catch null) |entry| {
        if (entry.kind != .file or count >= names.len) continue;
        @memcpy(names[count][0..entry.name.len], entry.name);
        // store length via a sentinel: use a parallel array
        count += 1;
        sub.deleteFile(entry.name) catch {};
    }
    sub.close();
    cwd.deleteDir(path) catch {};
}

test "checkpoint and reload" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_index_checkpoint";
    cleanupTestDir(cwd, dir_path);
    try cwd.createDir(dir_path, 0o755);
    defer cleanupTestDir(cwd, dir_path);

    const ins1 = [_]Change{.{ .insert = .{ .id = 1, .hashes = &[_]u32{ 100, 200, 300 } } }};
    const ins2 = [_]Change{.{ .insert = .{ .id = 2, .hashes = &[_]u32{ 100, 200, 300 } } }};

    // First open: insert enough to trigger a checkpoint (threshold 5 items).
    {
        const dir = try cwd.openDir(dir_path, .{});
        var index = try Self.open(std.testing.allocator, dir, 5);
        defer index.deinit();

        _ = try index.update(&ins1, null, null); // 3 items
        _ = try index.update(&ins2, null, null); // +3 -> checkpoint merges all memory

        // Checkpoint flushed both memory segments into one file segment.
        try std.testing.expectEqual(@as(usize, 1), index.file_segments.items.len);
        try std.testing.expectEqual(@as(usize, 0), index.memory_segments.items.len);

        var results = SearchResults.init(std.testing.allocator, .{ .max_results = 10, .min_score = 1 });
        defer results.deinit();
        var hashes = [_]u32{ 100, 200, 300 };
        try index.search(&hashes, &results);
        try std.testing.expectEqual(@as(u32, 3), results.hits.get(1).?.score);
        try std.testing.expectEqual(@as(u32, 3), results.hits.get(2).?.score);
    }

    // Reopen: file segment from manifest; oplog tail is fully covered.
    {
        const dir = try cwd.openDir(dir_path, .{});
        var index = try Self.open(std.testing.allocator, dir, 5);
        defer index.deinit();

        try std.testing.expectEqual(@as(usize, 1), index.file_segments.items.len);
        try std.testing.expectEqual(@as(usize, 0), index.memory_segments.items.len);
        try std.testing.expectEqual(@as(u64, 2), index.version);

        var results = SearchResults.init(std.testing.allocator, .{ .max_results = 10, .min_score = 1 });
        defer results.deinit();
        var hashes = [_]u32{ 100, 200, 300 };
        try index.search(&hashes, &results);
        try std.testing.expectEqual(@as(u32, 3), results.hits.get(1).?.score);
        try std.testing.expectEqual(@as(u32, 3), results.hits.get(2).?.score);
    }
}

test "file segment merging bounds count and preserves deletes" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_index_merge";
    cleanupTestDir(cwd, dir_path);
    try cwd.createDir(dir_path, 0o755);
    defer cleanupTestDir(cwd, dir_path);

    const dir = try cwd.openDir(dir_path, .{});
    var index = try Self.open(std.testing.allocator, dir, 1); // checkpoint every update
    defer index.deinit();

    // Each update becomes its own file segment; merging must keep the count down.
    var id: u32 = 1;
    while (id <= 30) : (id += 1) {
        const ins = [_]Change{.{ .insert = .{ .id = id, .hashes = &[_]u32{ 100, id } } }};
        _ = try index.update(&ins, null, null);
    }
    try std.testing.expect(index.file_segments.items.len < 30);

    // Delete one doc; the tombstone must shadow its insert across the merges.
    const del = [_]Change{.{ .delete = .{ .id = 5 } }};
    _ = try index.update(&del, null, null);

    var results = SearchResults.init(std.testing.allocator, .{ .max_results = 100, .min_score = 1 });
    defer results.deinit();
    var hashes = [_]u32{100};
    try index.search(&hashes, &results);

    const out = results.getResults();
    try std.testing.expectEqual(@as(usize, 29), out.len); // 30 inserted, 1 deleted
    for (out) |r| try std.testing.expect(r.id != 5);
}
