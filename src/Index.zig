// A single index: an on-disk oplog (WAL) plus in-memory and on-disk (file)
// segments. On open, file segments are loaded from the manifest and the oplog
// tail is replayed. When memory grows past a threshold, the oldest memory
// segment is checkpointed to a file segment. Merging of file segments comes
// later; for now each checkpoint produces one file segment.

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
    while (self.memory_segments.items.len > 0 and self.memoryItemCount() > self.checkpoint_threshold) {
        try self.checkpointOldest();
    }
}

// Write the oldest memory segment to a file segment, make it official in the
// manifest, then drop the memory segment. Crash-safe: the manifest write is the
// commit point, and the data stays in the oplog until then.
fn checkpointOldest(self: *Self) !void {
    const mem = self.memory_segments.items[0];

    var mem_reader = mem.reader();
    defer mem_reader.close();
    try filefmt.writeSegment(self.dir, &mem_reader, self.allocator);

    const fseg = try self.allocator.create(FileSegment);
    fseg.* = FileSegment.init(self.allocator);
    errdefer {
        fseg.deinit(.keep);
        self.allocator.destroy(fseg);
    }
    try filefmt.readSegment(self.dir, mem.info, fseg);

    try self.file_segments.append(self.allocator, fseg);
    errdefer _ = self.file_segments.pop();

    try self.writeManifest();

    // Commit point passed; now drop the memory segment.
    self.file_version = @max(self.file_version, mem.info.getLastCommitId());
    _ = self.memory_segments.orderedRemove(0);
    mem.deinit(.delete);
    self.allocator.destroy(mem);

    log.info("checkpointed segment {x} ({} items) to disk", .{ mem.info.version, fseg.num_items });
}

fn writeManifest(self: *Self) !void {
    var infos = try self.allocator.alloc(@import("segment.zig").SegmentInfo, self.file_segments.items.len);
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
        _ = try index.update(&ins2, null, null); // +3 -> checkpoint oldest

        try std.testing.expectEqual(@as(usize, 1), index.file_segments.items.len);
        try std.testing.expectEqual(@as(usize, 1), index.memory_segments.items.len);

        var results = SearchResults.init(std.testing.allocator, .{ .max_results = 10, .min_score = 1 });
        defer results.deinit();
        var hashes = [_]u32{ 100, 200, 300 };
        try index.search(&hashes, &results);
        try std.testing.expectEqual(@as(u32, 3), results.hits.get(1).?.score); // from file segment
        try std.testing.expectEqual(@as(u32, 3), results.hits.get(2).?.score); // from memory segment
    }

    // Reopen: file segment from manifest + oplog tail replay.
    {
        const dir = try cwd.openDir(dir_path, .{});
        var index = try Self.open(std.testing.allocator, dir, 5);
        defer index.deinit();

        try std.testing.expectEqual(@as(usize, 1), index.file_segments.items.len);
        try std.testing.expectEqual(@as(usize, 1), index.memory_segments.items.len);
        try std.testing.expectEqual(@as(u64, 2), index.version);

        var results = SearchResults.init(std.testing.allocator, .{ .max_results = 10, .min_score = 1 });
        defer results.deinit();
        var hashes = [_]u32{ 100, 200, 300 };
        try index.search(&hashes, &results);
        try std.testing.expectEqual(@as(u32, 3), results.hits.get(1).?.score);
        try std.testing.expectEqual(@as(u32, 3), results.hits.get(2).?.score);
    }
}
