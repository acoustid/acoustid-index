// A single index: an on-disk oplog (WAL) plus in-memory and on-disk (file)
// segments, held in an immutable refcounted snapshot (Segments). Readers acquire
// a snapshot and search it lock-free; the single writer builds a new snapshot
// (sharing unchanged segments via SharedPtr) and swaps it under a brief lock.
// A segment's file is deleted only when its last reference drops, so a merge
// never pulls a segment out from under an in-flight search.

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
const SharedPtr = @import("shared_ptr.zig").SharedPtr;
const KeepOrDelete = @import("common.zig").KeepOrDelete;
const log = std.log.scoped(.index);

const FileRef = SharedPtr(FileSegment);
const MemoryRef = SharedPtr(MemorySegment);

const Self = @This();

// Immutable snapshot of the index's segments. Refcounted via SharedPtr(Segments).
// Both lists are ordered oldest -> newest by version; file segments are older
// than all memory segments.
pub const Segments = struct {
    allocator: std.mem.Allocator,
    file: []FileRef,
    memory: []MemoryRef,
    version: u64 = 0,
    file_version: u64 = 0,

    // `delete` controls whether a segment whose refcount hits zero deletes its
    // backing file (.delete for segments dropped by a merge, .keep on shutdown).
    pub fn deinit(self: *Segments, delete: KeepOrDelete) void {
        for (self.memory) |*s| s.release(self.allocator, MemorySegment.deinit, .{.delete});
        for (self.file) |*s| s.release(self.allocator, FileSegment.deinit, .{delete});
        self.allocator.free(self.memory);
        self.allocator.free(self.file);
        self.* = undefined;
    }

    // Newest -> oldest across both lists (globally descending version). Segments
    // are non-merged per version, so info.version is the doc's version.
    pub fn hasNewerVersion(self: *const Segments, id: u32, version: u64) bool {
        var i = self.memory.len;
        while (i > 0) {
            i -= 1;
            const seg = self.memory[i].value;
            if (seg.info.version <= version) return false;
            if (id >= seg.min_doc_id and id <= seg.max_doc_id and seg.docs.contains(id)) return true;
        }
        var j = self.file.len;
        while (j > 0) {
            j -= 1;
            const seg = self.file[j].value;
            if (seg.info.version <= version) return false;
            if (id >= seg.min_doc_id and id <= seg.max_doc_id and seg.docs.contains(id)) return true;
        }
        return false;
    }
};

// A held snapshot. Search works on it without any lock.
pub const IndexReader = struct {
    allocator: std.mem.Allocator,
    snapshot: SharedPtr(Segments),

    pub fn deinit(self: *IndexReader) void {
        self.snapshot.release(self.allocator, Segments.deinit, .{.keep});
    }

    // `hashes` is sorted in place.
    pub fn search(self: *IndexReader, hashes: []u32, results: *SearchResults) !void {
        std.sort.pdq(u32, hashes, {}, std.sort.asc(u32));
        const segs = self.snapshot.value;
        for (segs.file) |seg| try seg.value.search(hashes, results);
        for (segs.memory) |seg| try seg.value.search(hashes, results);
        try results.finish(segs);
    }

    pub fn version(self: *const IndexReader) u64 {
        return self.snapshot.value.version;
    }
};

allocator: std.mem.Allocator,
// Index dir, split into two subdirs: `data_dir` holds the manifest + file
// segments, `oplog_dir` holds the WAL. The Index owns and closes all three.
dir: zio.Dir,
data_dir: zio.Dir,
oplog_dir: zio.Dir,
oplog: Oplog,

// Guards the `segments` pointer: readers acquire it shared (briefly), the writer
// swaps it exclusive (briefly). The actual search/merge work happens outside it.
segments_lock: zio.RwLock = .init,
// Serializes writers (only one may build+swap a snapshot at a time).
write_lock: zio.Mutex = .init,

// Current snapshot. Never mutated in place; replaced by the writer.
segments: SharedPtr(Segments),

// Writer-owned bookkeeping (stable under write_lock).
version: u64 = 0,
file_version: u64 = 0,
checkpoint_threshold: usize = 100_000,

// Background maintenance (checkpoint + file merges) runs on a dedicated per-index
// coroutine so it never blocks the update path. `wake` is a level-triggered
// "there is work" flag: set() coalesces (stays set until the worker resets it),
// and is safe to call any time — no dependency on the coroutine running.
wake: zio.ResetEvent = .init,
stopping: std.atomic.Value(bool) = .init(false),
maintenance: ?zio.JoinHandle(void) = null,

pub fn open(allocator: std.mem.Allocator, dir: zio.Dir, checkpoint_threshold: usize) !Self {
    var file_list: std.ArrayListUnmanaged(FileRef) = .empty;
    var mem_list: std.ArrayListUnmanaged(MemoryRef) = .empty;
    errdefer {
        for (file_list.items) |*s| s.release(allocator, FileSegment.deinit, .{.keep});
        file_list.deinit(allocator);
        for (mem_list.items) |*s| s.release(allocator, MemorySegment.deinit, .{.delete});
        mem_list.deinit(allocator);
    }

    // The WAL and the data files live in separate subdirs.
    const data_dir = try openOrCreateDir(dir, "data");
    errdefer data_dir.close();
    const oplog_dir = try openOrCreateDir(dir, "oplog");
    errdefer oplog_dir.close();

    // 1. Load file segments listed in the manifest.
    var file_version: u64 = 0;
    const infos = try manifest.read(data_dir, allocator);
    defer allocator.free(infos);
    for (infos) |info| {
        var ref = try FileRef.create(allocator, FileSegment.init(allocator));
        {
            errdefer ref.release(allocator, FileSegment.deinit, .{.keep});
            try filefmt.readSegment(data_dir, info, ref.value);
        }
        try file_list.append(allocator, ref);
        file_version = @max(file_version, info.getLastCommitId());
    }

    // 2. Open the oplog and replay only the tail (versions > file_version).
    var ctx = ReplayCtx{ .allocator = allocator, .mem_list = &mem_list, .file_version = file_version };
    var oplog = try Oplog.open(allocator, oplog_dir, &ctx, ReplayCtx.apply);
    errdefer oplog.deinit();

    const version = @max(file_version, oplog.last_version);

    const segments = try SharedPtr(Segments).create(allocator, .{
        .allocator = allocator,
        .file = try file_list.toOwnedSlice(allocator),
        .memory = try mem_list.toOwnedSlice(allocator),
        .version = version,
        .file_version = file_version,
    });

    return .{
        .allocator = allocator,
        .dir = dir,
        .data_dir = data_dir,
        .oplog_dir = oplog_dir,
        .oplog = oplog,
        .segments = segments,
        .version = version,
        .file_version = file_version,
        .checkpoint_threshold = checkpoint_threshold,
    };
}

fn openOrCreateDir(parent: zio.Dir, name: []const u8) !zio.Dir {
    parent.createDir(name, 0o755) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };
    return parent.openDir(name, .{ .iterate = true });
}

const ReplayCtx = struct {
    allocator: std.mem.Allocator,
    mem_list: *std.ArrayListUnmanaged(MemoryRef),
    file_version: u64,

    fn apply(self: *ReplayCtx, txn: Transaction) !void {
        if (txn.id <= self.file_version) return; // already in a file segment
        var ref = try MemoryRef.create(self.allocator, MemorySegment.init(self.allocator, .{}));
        errdefer ref.release(self.allocator, MemorySegment.deinit, .{.delete});
        try ref.value.build(txn.changes, txn.metadata);
        ref.value.info = .{ .version = txn.id, .merges = 0 };
        try self.mem_list.append(self.allocator, ref);
    }
};

pub fn deinit(self: *Self) void {
    self.stop();
    self.oplog.deinit();
    self.segments.release(self.allocator, Segments.deinit, .{.keep});
    self.oplog_dir.close();
    self.data_dir.close();
    self.dir.close();
}

/// Acquire a consistent snapshot to search. Caller must deinit it.
pub fn acquireReader(self: *Self) IndexReader {
    self.segments_lock.lockShared() catch {};
    defer self.segments_lock.unlockShared();
    return .{ .allocator = self.allocator, .snapshot = self.segments.acquire() };
}

// Install a new snapshot, swap it in, and release the old one (dropping the
// files of any segment no longer referenced). Takes ownership of file/memory on
// success; on OOM leaves them for the caller to release.
fn installSnapshot(self: *Self, file: []FileRef, memory: []MemoryRef, version: u64, file_version: u64) !void {
    const snap = try SharedPtr(Segments).create(self.allocator, .{
        .allocator = self.allocator,
        .file = file,
        .memory = memory,
        .version = version,
        .file_version = file_version,
    });

    self.segments_lock.lock() catch {};
    var old = self.segments;
    self.segments = snap;
    self.segments_lock.unlock();

    old.release(self.allocator, Segments.deinit, .{.delete});
    self.version = version;
    self.file_version = file_version;
}

fn cloneRefs(comptime T: type, allocator: std.mem.Allocator, src: []SharedPtr(T)) ![]SharedPtr(T) {
    const dst = try allocator.alloc(SharedPtr(T), src.len);
    for (src, 0..) |s, i| dst[i] = s.acquire();
    return dst;
}

fn releaseRefs(comptime T: type, allocator: std.mem.Allocator, refs: []SharedPtr(T), cleanup: KeepOrDelete) void {
    for (refs) |*s| s.release(allocator, T.deinit, .{cleanup});
    allocator.free(refs);
}

// Writer path. Build the memory segment before the durable append so a build
// failure never leaves the log ahead of memory; the oplog append is the commit
// point.
pub fn update(self: *Self, changes: []const Change, metadata: ?Metadata, expected_version: ?u64) !u64 {
    self.write_lock.lock() catch {};
    defer self.write_lock.unlock();

    var seg = try MemoryRef.create(self.allocator, MemorySegment.init(self.allocator, .{}));
    var seg_consumed = false;
    errdefer if (!seg_consumed) seg.release(self.allocator, MemorySegment.deinit, .{.delete});
    try seg.value.build(changes, metadata);

    const version = try self.oplog.append(changes, metadata, expected_version);
    seg.value.info = .{ .version = version, .merges = 0 };

    const cur = self.segments.value;
    const new_file = try cloneRefs(FileSegment, self.allocator, cur.file);
    var arrays_consumed = false;
    errdefer if (!arrays_consumed) releaseRefs(FileSegment, self.allocator, new_file, .keep);

    const new_memory = try self.allocator.alloc(MemoryRef, cur.memory.len + 1);
    errdefer if (!arrays_consumed) self.allocator.free(new_memory);
    for (cur.memory, 0..) |s, i| new_memory[i] = s.acquire();
    new_memory[cur.memory.len] = seg;
    seg_consumed = true;

    try self.installSnapshot(new_file, new_memory, version, self.file_version);
    arrays_consumed = true;

    // Any update may create maintenance work (memory merge, then checkpoint,
    // then file merge). Signal the coroutine; it decides what's actually needed.
    // Level-triggered, so it's safe even when the coroutine isn't running (tests
    // drive runMaintenance() synchronously instead).
    self.wake.set();
    return version;
}

/// Start the background maintenance coroutine. Call once the Index is at its
/// final address (the coroutine captures `self`).
pub fn start(self: *Self) !void {
    self.maintenance = try zio.spawn(maintenanceLoop, .{self});
}

/// Stop the maintenance coroutine and wait for it to finish. Call before deinit.
pub fn stop(self: *Self) void {
    if (self.maintenance) |*task| {
        self.stopping.store(true, .release);
        self.wake.set();
        task.join();
        self.maintenance = null;
    }
}

fn maintenanceLoop(self: *Self) void {
    while (true) {
        self.wake.wait() catch return;
        self.wake.reset(); // reset before processing so a set() during the pass isn't lost
        if (self.stopping.load(.acquire)) return;
        self.runMaintenance() catch |err| {
            log.warn("maintenance failed: {}", .{err});
        };
    }
}

// Cascade all pending work until nothing is left: consolidate memory segments,
// flush memory to a file segment, then merge file segments. A concurrent update
// may add work again, which re-signals the wake flag.
fn runMaintenance(self: *Self) !void {
    while (true) {
        if (try self.mergeMemory()) continue;
        if (try self.checkpoint()) continue;
        if (try self.mergeFiles()) continue;
        break;
    }
}

fn memoryItemCount(self: *const Self) usize {
    var total: usize = 0;
    for (self.segments.value.memory) |seg| total += seg.value.getSize();
    return total;
}

fn memorySize(memory: []const MemoryRef) usize {
    var total: usize = 0;
    for (memory) |seg| total += seg.value.getSize();
    return total;
}

fn fileSegmentSize(seg: FileRef) usize {
    return seg.value.getSize();
}

fn fileSegmentFrozen(seg: FileRef) bool {
    _ = seg;
    return false;
}

fn memorySegmentSize(seg: MemoryRef) usize {
    return seg.value.getSize();
}

fn memorySegmentFrozen(seg: MemoryRef) bool {
    _ = seg;
    return false;
}

// Consolidate a tiered-policy-selected range of memory segments into one new
// in-memory segment (no disk). Cuts how many segments a search scans between
// checkpoints. Same phase split as the others: merge lock-free, swap under the
// write lock. Returns true if a merge ran.
fn mergeMemory(self: *Self) !bool {
    const policy = TieredMergePolicy(MemoryRef, memorySegmentSize, memorySegmentFrozen){
        .min_segment_size = 100,
        .max_segment_size = self.checkpoint_threshold,
        .segments_per_merge = 10,
        .segments_per_level = 5,
        // Cap memory at ~16 segments before merging (matches the old design);
        // without this the geometric budget merges far too aggressively.
        .max_segments = 16,
    };

    self.segments_lock.lockShared() catch {};
    var snap = self.segments.acquire();
    self.segments_lock.unlockShared();
    defer snap.release(self.allocator, Segments.deinit, .{.keep});

    const src_mem = snap.value.memory;
    if (src_mem.len <= policy.calculateBudget(src_mem)) return false;
    const candidate = policy.findSegmentsToMerge(src_mem) orelse return false;
    const lo = candidate.start;
    const hi = candidate.end;
    if (hi - lo < 2) return false;
    const n = hi - lo;

    var merged = try MemoryRef.create(self.allocator, MemorySegment.init(self.allocator, .{}));
    var merged_placed = false;
    var installed = false;
    errdefer if (!installed and !merged_placed) merged.release(self.allocator, MemorySegment.deinit, .{.delete});
    {
        var merger = try SegmentMerger(MemorySegment).init(self.allocator, n);
        defer merger.deinit();
        for (src_mem[lo..hi]) |s| merger.addSource(s.value);
        try merger.prepare(snap.value);
        try merged.value.buildFromMerger(&merger);
    }

    self.write_lock.lock() catch {};
    defer self.write_lock.unlock();

    // Existing memory segments don't move (updates only append), so lo/hi stay
    // valid; the suffix picks up updates that arrived during the merge.
    const cur = self.segments.value;
    const new_memory = try self.allocator.alloc(MemoryRef, cur.memory.len - n + 1);
    var nm: usize = 0;
    errdefer if (!installed) {
        for (new_memory[0..nm]) |*s| s.release(self.allocator, MemorySegment.deinit, .{.delete});
        self.allocator.free(new_memory);
    };
    for (cur.memory[0..lo]) |s| {
        new_memory[nm] = s.acquire();
        nm += 1;
    }
    new_memory[nm] = merged;
    nm += 1;
    merged_placed = true;
    for (cur.memory[hi..]) |s| {
        new_memory[nm] = s.acquire();
        nm += 1;
    }

    const new_file = try cloneRefs(FileSegment, self.allocator, cur.file);
    errdefer if (!installed) releaseRefs(FileSegment, self.allocator, new_file, .keep);

    try self.installSnapshot(new_file, new_memory, self.version, self.file_version);
    installed = true;

    log.info("merged {} memory segments -> {x}-{x} ({} items)", .{ n, merged.value.info.version, merged.value.info.merges, merged.value.getSize() });
    return true;
}

// Flush all memory segments to one file segment. The merge runs without the
// write lock; only the manifest write + snapshot swap hold it, so updates keep
// flowing. Updates that arrive during the merge stay in memory (they append to
// the suffix; the flushed segments are the prefix). Returns true if it ran.
fn checkpoint(self: *Self) !bool {
    self.segments_lock.lockShared() catch {};
    var snap = self.segments.acquire();
    self.segments_lock.unlockShared();
    defer snap.release(self.allocator, Segments.deinit, .{.keep});

    const flush_count = snap.value.memory.len;
    if (flush_count == 0 or memorySize(snap.value.memory) <= self.checkpoint_threshold) return false;

    var fseg = try self.mergeToFileSegment(MemorySegment, snap.value.memory, snap.value);
    var installed = false;
    var fseg_placed = false;
    errdefer if (!installed and !fseg_placed) fseg.release(self.allocator, FileSegment.deinit, .{.delete});
    const info = fseg.value.info;

    self.write_lock.lock() catch {};
    defer self.write_lock.unlock();

    const cur = self.segments.value;
    const kept = cur.memory[flush_count..];

    const new_file = try self.allocator.alloc(FileRef, cur.file.len + 1);
    var nf: usize = 0;
    errdefer if (!installed) {
        for (new_file[0..nf]) |*s| s.release(self.allocator, FileSegment.deinit, .{.delete});
        self.allocator.free(new_file);
    };
    for (cur.file) |s| {
        new_file[nf] = s.acquire();
        nf += 1;
    }
    new_file[nf] = fseg;
    nf += 1;
    fseg_placed = true;

    const new_memory = try cloneRefs(MemorySegment, self.allocator, kept);
    errdefer if (!installed) releaseRefs(MemorySegment, self.allocator, new_memory, .keep);

    try self.writeManifestFor(new_file);
    try self.installSnapshot(new_file, new_memory, self.version, @max(self.file_version, info.getLastCommitId()));
    installed = true;

    // Transactions up to file_version are now durable in file segments; drop the
    // oplog files entirely below it. (After the manifest commit, so a crash in
    // between just leaves redundant oplog entries that replay skips.)
    self.oplog.truncate(self.file_version) catch |err| {
        log.warn("oplog truncate failed: {}", .{err});
    };

    log.info("checkpointed to file segment {x}-{x} ({} items)", .{ info.version, info.merges, fseg.value.num_items });
    return true;
}

// Merge a tiered-policy-selected range of file segments into one. Same phase
// split: the merge is lock-free, only the swap holds the write lock. The
// merged-away files are deleted when the old snapshot's last reference drops
// (FileSegment.deinit(.delete)), so in-flight readers keep them until done.
// Returns true if a merge ran.
fn mergeFiles(self: *Self) !bool {
    const policy = TieredMergePolicy(FileRef, fileSegmentSize, fileSegmentFrozen){
        .min_segment_size = 100,
        .max_segment_size = 1_000_000_000,
        .segments_per_merge = 10,
        .segments_per_level = 10,
    };

    self.segments_lock.lockShared() catch {};
    var snap = self.segments.acquire();
    self.segments_lock.unlockShared();
    defer snap.release(self.allocator, Segments.deinit, .{.keep});

    const src_file = snap.value.file;
    if (src_file.len <= policy.calculateBudget(src_file)) return false;
    const candidate = policy.findSegmentsToMerge(src_file) orelse return false;
    const lo = candidate.start;
    const hi = candidate.end;
    if (hi - lo < 2) return false;
    const n = hi - lo;

    var fseg = try self.mergeToFileSegment(FileSegment, src_file[lo..hi], snap.value);
    var installed = false;
    var fseg_placed = false;
    errdefer if (!installed and !fseg_placed) fseg.release(self.allocator, FileSegment.deinit, .{.delete});
    const info = fseg.value.info;

    self.write_lock.lock() catch {};
    defer self.write_lock.unlock();

    // File segments are unchanged since the capture (only this coroutine touches
    // them), so start/end are still valid; memory may have grown.
    const cur = self.segments.value;
    const new_file = try self.allocator.alloc(FileRef, cur.file.len - n + 1);
    var nf: usize = 0;
    errdefer if (!installed) {
        for (new_file[0..nf]) |*s| s.release(self.allocator, FileSegment.deinit, .{.delete});
        self.allocator.free(new_file);
    };
    for (cur.file[0..lo]) |s| {
        new_file[nf] = s.acquire();
        nf += 1;
    }
    new_file[nf] = fseg;
    nf += 1;
    fseg_placed = true;
    for (cur.file[hi..]) |s| {
        new_file[nf] = s.acquire();
        nf += 1;
    }

    const new_memory = try cloneRefs(MemorySegment, self.allocator, cur.memory);
    errdefer if (!installed) releaseRefs(MemorySegment, self.allocator, new_memory, .keep);

    try self.writeManifestFor(new_file);
    try self.installSnapshot(new_file, new_memory, self.version, self.file_version);
    installed = true;

    log.info("merged {} file segments -> {x}-{x} ({} items)", .{ n, info.version, info.merges, fseg.value.num_items });
    return true;
}

// Merge `sources` into a new on-disk file segment and load it back. `collection`
// provides hasNewerVersion (the current snapshot). On error the written file is
// removed. Caller owns the returned ref.
fn mergeToFileSegment(self: *Self, comptime Segment: type, sources: []SharedPtr(Segment), collection: anytype) !FileRef {
    var merger = try SegmentMerger(Segment).init(self.allocator, sources.len);
    defer merger.deinit();
    for (sources) |s| merger.addSource(s.value);
    try merger.prepare(collection);
    const info = merger.segment.info;

    try filefmt.writeSegment(self.data_dir, &merger, self.allocator);
    errdefer filefmt.deleteSegmentFile(self.data_dir, info) catch {};

    var ref = try FileRef.create(self.allocator, FileSegment.init(self.allocator));
    errdefer ref.release(self.allocator, FileSegment.deinit, .{.delete});
    try filefmt.readSegment(self.data_dir, info, ref.value);
    return ref;
}

fn writeManifestFor(self: *Self, file: []const FileRef) !void {
    const infos = try self.allocator.alloc(SegmentInfo, file.len);
    defer self.allocator.free(infos);
    for (file, 0..) |seg, i| infos[i] = seg.value.info;
    try manifest.write(self.data_dir, self.allocator, infos);
}

fn cleanupTestDir(cwd: zio.Dir, path: []const u8) void {
    @import("common.zig").deleteDirTree(std.testing.allocator, cwd, path) catch {};
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

    {
        const dir = try cwd.openDir(dir_path, .{ .iterate = true });
        var index = try Self.open(std.testing.allocator, dir, 5);
        defer index.deinit();

        _ = try index.update(&ins1, null, null);
        _ = try index.update(&ins2, null, null);
        try index.runMaintenance(); // flush memory -> file segment

        try std.testing.expectEqual(@as(usize, 1), index.segments.value.file.len);
        try std.testing.expectEqual(@as(usize, 0), index.segments.value.memory.len);

        var results = SearchResults.init(std.testing.allocator, .{ .max_results = 10, .min_score = 1 });
        defer results.deinit();
        var reader = index.acquireReader();
        defer reader.deinit();
        var hashes = [_]u32{ 100, 200, 300 };
        try reader.search(&hashes, &results);
        try std.testing.expectEqual(@as(u32, 3), results.hits.get(1).?.score);
        try std.testing.expectEqual(@as(u32, 3), results.hits.get(2).?.score);
    }

    {
        const dir = try cwd.openDir(dir_path, .{ .iterate = true });
        var index = try Self.open(std.testing.allocator, dir, 5);
        defer index.deinit();

        try std.testing.expectEqual(@as(usize, 1), index.segments.value.file.len);
        try std.testing.expectEqual(@as(usize, 0), index.segments.value.memory.len);
        try std.testing.expectEqual(@as(u64, 2), index.version);

        var results = SearchResults.init(std.testing.allocator, .{ .max_results = 10, .min_score = 1 });
        defer results.deinit();
        var reader = index.acquireReader();
        defer reader.deinit();
        var hashes = [_]u32{ 100, 200, 300 };
        try reader.search(&hashes, &results);
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

    const dir = try cwd.openDir(dir_path, .{ .iterate = true });
    var index = try Self.open(std.testing.allocator, dir, 1);
    defer index.deinit();

    var id: u32 = 1;
    while (id <= 30) : (id += 1) {
        const ins = [_]Change{.{ .insert = .{ .id = id, .hashes = &[_]u32{ 100, id } } }};
        _ = try index.update(&ins, null, null);
        try index.runMaintenance();
    }
    try std.testing.expect(index.segments.value.file.len < 30);

    const del = [_]Change{.{ .delete = .{ .id = 5 } }};
    _ = try index.update(&del, null, null);

    var results = SearchResults.init(std.testing.allocator, .{ .max_results = 100, .min_score = 1 });
    defer results.deinit();
    var reader = index.acquireReader();
    defer reader.deinit();
    var hashes = [_]u32{100};
    try reader.search(&hashes, &results);

    const out = results.getResults();
    try std.testing.expectEqual(@as(usize, 29), out.len);
    for (out) |r| try std.testing.expect(r.id != 5);
}

test "reader snapshot is stable across writes" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_index_snapshot";
    cleanupTestDir(cwd, dir_path);
    try cwd.createDir(dir_path, 0o755);
    defer cleanupTestDir(cwd, dir_path);

    const dir = try cwd.openDir(dir_path, .{ .iterate = true });
    var index = try Self.open(std.testing.allocator, dir, 5);
    defer index.deinit();

    _ = try index.update(&[_]Change{.{ .insert = .{ .id = 1, .hashes = &[_]u32{100} } }}, null, null);

    // Snapshot taken now sees only id 1.
    var reader = index.acquireReader();
    defer reader.deinit();

    // Many more writes trigger checkpoints + merges that free the segments the
    // new snapshots no longer reference; the old reader must stay valid.
    var id: u32 = 2;
    while (id <= 30) : (id += 1) {
        _ = try index.update(&[_]Change{.{ .insert = .{ .id = id, .hashes = &[_]u32{100} } }}, null, null);
        try index.runMaintenance();
    }

    var r1 = SearchResults.init(std.testing.allocator, .{ .max_results = 100, .min_score = 1 });
    defer r1.deinit();
    var h1 = [_]u32{100};
    try reader.search(&h1, &r1);
    try std.testing.expectEqual(@as(usize, 1), r1.getResults().len); // snapshot isolation

    var r2 = SearchResults.init(std.testing.allocator, .{ .max_results = 100, .min_score = 1 });
    defer r2.deinit();
    var fresh = index.acquireReader();
    defer fresh.deinit();
    var h2 = [_]u32{100};
    try fresh.search(&h2, &r2);
    try std.testing.expectEqual(@as(usize, 30), r2.getResults().len);
}

test "memory merge consolidates memory segments without checkpointing" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_index_memmerge";
    cleanupTestDir(cwd, dir_path);
    try cwd.createDir(dir_path, 0o755);
    defer cleanupTestDir(cwd, dir_path);

    const dir = try cwd.openDir(dir_path, .{ .iterate = true });
    var index = try Self.open(std.testing.allocator, dir, 100_000); // high threshold: no checkpoint
    defer index.deinit();

    // 50 tiny updates stay well under the checkpoint threshold.
    var id: u32 = 1;
    while (id <= 50) : (id += 1) {
        _ = try index.update(&[_]Change{.{ .insert = .{ .id = id, .hashes = &[_]u32{id} } }}, null, null);
        try index.runMaintenance();
    }

    // No checkpoint (below threshold), but memory merged into far fewer segments.
    try std.testing.expectEqual(@as(usize, 0), index.segments.value.file.len);
    try std.testing.expect(index.segments.value.memory.len < 50);

    // Everything still searchable.
    var results = SearchResults.init(std.testing.allocator, .{ .max_results = 100, .min_score = 1 });
    defer results.deinit();
    var reader = index.acquireReader();
    defer reader.deinit();
    var h = [_]u32{25};
    try reader.search(&h, &results);
    try std.testing.expectEqual(@as(usize, 1), results.getResults().len);
}

test "oplog truncation after checkpoint" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_oplog_truncate";
    cleanupTestDir(cwd, dir_path);
    try cwd.createDir(dir_path, 0o755);
    defer cleanupTestDir(cwd, dir_path);

    {
        const dir = try cwd.openDir(dir_path, .{ .iterate = true });
        var index = try Self.open(std.testing.allocator, dir, 10);
        defer index.deinit();
        index.oplog.max_file_size = 80; // force frequent rotation

        var id: u32 = 1;
        while (id <= 40) : (id += 1) {
            _ = try index.update(&[_]Change{.{ .insert = .{ .id = id, .hashes = &[_]u32{ 100, id } } }}, null, null);
            try index.runMaintenance();
        }
    }

    // Truncation should have deleted the oplog files below the checkpoints.
    {
        var dir = try cwd.openDir(dir_path, .{ .iterate = true });
        defer dir.close();
        var n: usize = 0;
        var it = dir.iterate();
        while (try it.next()) |e| {
            if (e.kind == .file and std.mem.endsWith(u8, e.name, ".xlog")) n += 1;
        }
        try std.testing.expect(n < 15);
    }

    // Reload from the truncated log + file segments; data intact.
    {
        const dir = try cwd.openDir(dir_path, .{ .iterate = true });
        var index = try Self.open(std.testing.allocator, dir, 10);
        defer index.deinit();

        var results = SearchResults.init(std.testing.allocator, .{ .max_results = 100, .min_score = 1 });
        defer results.deinit();
        var reader = index.acquireReader();
        defer reader.deinit();
        var h = [_]u32{40}; // id 40's unique hash
        try reader.search(&h, &results);
        try std.testing.expectEqual(@as(usize, 1), results.getResults().len);
    }
}
