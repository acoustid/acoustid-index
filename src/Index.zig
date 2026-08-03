// A single index: an on-disk oplog (WAL) plus in-memory and on-disk (file)
// segments, held in an immutable refcounted snapshot (Segments). Readers acquire
// a snapshot and search it lock-free; the single writer builds a new snapshot
// (sharing unchanged segments via SharedPtr) and swaps it under a brief lock.
// A segment's file is deleted only when its last reference drops, so a merge
// never pulls a segment out from under an in-flight search.

const std = @import("std");
const zio = @import("zio");
const Change = @import("change.zig").Change;
const MetadataEntry = @import("change.zig").MetadataEntry;
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
const metrics = @import("metrics.zig");
const SharedPtr = @import("shared_ptr.zig").SharedPtr;
const DocInfo = @import("common.zig").DocInfo;
const log = std.log.scoped(.index);

const FileRef = SharedPtr(FileSegment);
const MemoryRef = SharedPtr(MemorySegment);

const Self = @This();

// Immutable snapshot of the index's segments. Refcounted via SharedPtr(Segments).
// Both lists are ordered oldest -> newest by commit_id; file segments are older
// than all memory segments.
pub const Segments = struct {
    allocator: std.mem.Allocator,
    file: []FileRef,
    memory: []MemoryRef,
    // Internal commit ids: `commit_id` is the newest committed, `file_commit_id` the
    // newest durable in a file segment (what oplog replay filters on).
    commit_id: u64 = 0,
    file_commit_id: u64 = 0,
    // The same two points expressed as upstream changelog positions: `version`
    // is what a restarted node resumes from, `file_version` what a snapshot of
    // this index resumes from (it carries file segments only). See SegmentInfo.
    version: u64 = 0,
    file_version: u64 = 0,
    // Whether this index is fed by an upstream log; rides along so a snapshot taken
    // from this reader carries it to whoever restores it. See Oplog.append.
    external_versions: bool = false,

    // Drop every segment ref. A file whose last reference this releases is deleted
    // iff the segment was retired by a merge (FileSegment.delete_on_destroy) — so
    // shutdown keeps live segments and a merge deletes the ones it replaced, without
    // any per-release keep/delete decision.
    pub fn deinit(self: *Segments) void {
        for (self.memory) |*s| s.release(self.allocator, MemorySegment.deinit, .{});
        for (self.file) |*s| s.release(self.allocator, FileSegment.deinit, .{});
        self.allocator.free(self.memory);
        self.allocator.free(self.file);
        self.* = undefined;
    }

    // The newest segment that mentions `id` wins (segments partition the commit-id
    // space), giving the version that doc was last written at and whether it's a
    // tombstone. Returns null if no segment mentions the id at all.
    pub fn getDocInfo(self: *const Segments, id: u32) ?DocInfo {
        var i = self.memory.len;
        while (i > 0) {
            i -= 1;
            const seg = self.memory[i].value;
            if (id >= seg.min_doc_id and id <= seg.max_doc_id) {
                if (seg.docs.get(id)) |alive| return .{ .version = seg.info.effectiveVersion(), .deleted = !alive };
            }
        }
        var j = self.file.len;
        while (j > 0) {
            j -= 1;
            const seg = self.file[j].value;
            if (id >= seg.min_doc_id and id <= seg.max_doc_id) {
                if (seg.docs.get(id)) |alive| return .{ .version = seg.info.effectiveVersion(), .deleted = !alive };
            }
        }
        return null;
    }

    pub fn numSegments(self: *const Segments) usize {
        return self.file.len + self.memory.len;
    }

    // Sum of per-segment doc counts (approximate: counts tombstones and a doc
    // present in several segments more than once). Matches the old semantics.
    pub fn numDocs(self: *const Segments) u32 {
        var n: u32 = 0;
        for (self.file) |s| n += @intCast(s.value.docs.count());
        for (self.memory) |s| n += @intCast(s.value.docs.count());
        return n;
    }

    pub fn minDocId(self: *const Segments) u32 {
        var result: u32 = 0;
        for (self.file) |s| {
            const m = s.value.min_doc_id;
            if (m != 0 and (result == 0 or m < result)) result = m;
        }
        for (self.memory) |s| {
            const m = s.value.min_doc_id;
            if (m != 0 and (result == 0 or m < result)) result = m;
        }
        return result;
    }

    pub fn maxDocId(self: *const Segments) u32 {
        var result: u32 = 0;
        for (self.file) |s| result = @max(result, s.value.max_doc_id);
        for (self.memory) |s| result = @max(result, s.value.max_doc_id);
        return result;
    }

    // Merge every segment's metadata oldest -> newest (newest key wins), into a
    // fresh owned map on `arena`, so it outlives the reader snapshot.
    pub fn buildMetadata(self: *const Segments, arena: std.mem.Allocator) !Metadata {
        var md = Metadata.initOwned(arena);
        errdefer md.deinit();
        for (self.file) |s| try md.update(s.value.metadata);
        for (self.memory) |s| try md.update(s.value.metadata);
        return md;
    }

    // Newest -> oldest across both lists (globally descending commit_id). Segments
    // are non-merged per commit_id, so info.commit_id is the doc's commit_id.
    pub fn hasNewerCommit(self: *const Segments, id: u32, commit_id: u64) bool {
        var i = self.memory.len;
        while (i > 0) {
            i -= 1;
            const seg = self.memory[i].value;
            if (seg.info.commit_id <= commit_id) return false;
            if (id >= seg.min_doc_id and id <= seg.max_doc_id and seg.docs.contains(id)) return true;
        }
        var j = self.file.len;
        while (j > 0) {
            j -= 1;
            const seg = self.file[j].value;
            if (seg.info.commit_id <= commit_id) return false;
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
        // If this search outlived the merge that superseded its snapshot, it may be
        // the last holder of a retired segment — deinit deletes that segment's file
        // (delete_on_destroy), so the merge's cleanup completes here rather than
        // leaking. Live segments aren't marked, so nothing live is touched.
        self.snapshot.release(self.allocator, Segments.deinit, .{});
    }

    // `hashes` is sorted and de-duplicated in place. The query is conceptually a
    // set: MemorySegment.search counts a repeated hash once (it advances past
    // consumed items) but FileSegment.search would count it twice (it re-probes the
    // same block), so a duplicate hash scores a doc differently before vs after a
    // checkpoint. Dedupe up front so both agree (and skip redundant probes).
    pub fn search(self: *IndexReader, hashes: []u32, results: *SearchResults) !void {
        std.sort.pdq(u32, hashes, {}, std.sort.asc(u32));
        const query = dedupSorted(hashes);
        const segs = self.snapshot.value;
        for (segs.file) |seg| try seg.value.search(query, results);
        for (segs.memory) |seg| try seg.value.search(query, results);
        try results.finish(segs);
    }

    /// The upstream changelog position this snapshot reflects — what callers, and the
    /// API, mean by "the index version". Not the internal commit id; see SegmentInfo.
    pub fn version(self: *const IndexReader) u64 {
        return self.snapshot.value.version;
    }

    /// Current state of a doc id in this snapshot, or null if never seen. A
    /// tombstone (deleted) is reported as `.deleted = true`.
    pub fn getDocInfo(self: *const IndexReader, id: u32) ?DocInfo {
        return self.snapshot.value.getDocInfo(id);
    }

    pub fn numSegments(self: *const IndexReader) usize {
        return self.snapshot.value.numSegments();
    }
    pub fn numDocs(self: *const IndexReader) u32 {
        return self.snapshot.value.numDocs();
    }
    pub fn minDocId(self: *const IndexReader) u32 {
        return self.snapshot.value.minDocId();
    }
    pub fn maxDocId(self: *const IndexReader) u32 {
        return self.snapshot.value.maxDocId();
    }
    pub fn buildMetadata(self: *const IndexReader, arena: std.mem.Allocator) !Metadata {
        return self.snapshot.value.buildMetadata(arena);
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
// Internal: dense commit ids. `file_commit_id` is what oplog replay and truncation
// key on. See SegmentInfo.
commit_id: u64 = 0,
file_commit_id: u64 = 0,
// Derived at open from the segments and the replayed WAL, then kept up to date by
// update(): true once anything in this index carries an upstream position.
external_versions: bool = false,
// External: upstream changelog positions. `version` is the resume point;
// `file_version` is the watermark a snapshot of this index carries (file segments
// only), so it is what a peer bootstrapping from here resumes at.
version: u64 = 0,
file_version: u64 = 0,
checkpoint_threshold: usize = 100_000,
// Force a checkpoint once the oldest uncheckpointed write is this old, even below the
// size threshold — bounds WAL growth and keeps file_version fresh (so it stays within
// the coordinator's changelog retention for snapshot bootstrap). null disables it.
checkpoint_age: ?zio.Duration = null,
// When maintenance first saw the current uncheckpointed memory (null = none). Touched
// only by checkpoint(), which runs solely on the maintenance coroutine, so it needs
// no lock or atomic.
pending_since: ?zio.Timestamp = null,

// Background maintenance (checkpoint + file merges) runs on a dedicated per-index
// coroutine so it never blocks the update path. `wake` is a level-triggered
// "there is work" flag: set() coalesces (stays set until the worker resets it),
// and is safe to call any time — no dependency on the coroutine running.
wake: zio.ResetEvent = .init,
maintenance: ?zio.JoinHandle(zio.Cancelable!void) = null,

pub fn open(allocator: std.mem.Allocator, dir: zio.Dir, checkpoint_threshold: usize, sync: bool, load_sem: ?*zio.Semaphore) !Self {
    var file_list: std.ArrayListUnmanaged(FileRef) = .empty;
    var mem_list: std.ArrayListUnmanaged(MemoryRef) = .empty;
    errdefer {
        for (file_list.items) |*s| s.release(allocator, FileSegment.deinit, .{});
        file_list.deinit(allocator);
        for (mem_list.items) |*s| s.release(allocator, MemorySegment.deinit, .{});
        mem_list.deinit(allocator);
    }

    // The WAL and the data files live in separate subdirs.
    const data_dir = try openOrCreateDir(dir, "data");
    errdefer data_dir.close();
    const oplog_dir = try openOrCreateDir(dir, "oplog");
    errdefer oplog_dir.close();

    // 1. Load file segments listed in the manifest, concurrently: readSegment is
    //    IO-bound (reads the whole file via async IO), so overlapping the loads
    //    keeps many reads in flight through io_uring. Each loader writes its own
    //    slot, so results stay in manifest order.
    var file_commit_id: u64 = 0;
    var file_version: u64 = 0;
    // Upstream-fed is derived, not stored: any segment carrying a position marks it.
    // The manifest and the WAL cover each other — a checkpoint truncates the WAL only
    // once its transactions are durable in a segment, which then carries the position.
    var external_versions: bool = false;
    const infos = try manifest.read(data_dir, allocator);
    defer allocator.free(infos);

    const refs = try allocator.alloc(FileRef, infos.len);
    defer allocator.free(refs);
    const results = try allocator.alloc(anyerror!void, infos.len);
    defer allocator.free(results);
    for (results) |*r| r.* = error.SegmentNotLoaded; // sentinel for slots never spawned

    // Reserve up front so collection can't fail (and leak) after the loads.
    try file_list.ensureTotalCapacity(allocator, infos.len);

    var fatal: ?anyerror = null;
    {
        var group: zio.Group = .init;
        defer group.cancel(); // cancel+join any stragglers on early exit
        for (infos, 0..) |info, i| {
            // Acquire a permit BEFORE spawning, so at most `permits` loads are
            // ever live at once — this bounds spawned coroutines (and their
            // in-memory buffers), not just concurrent reads. The loader releases
            // the permit when it finishes.
            if (load_sem) |sem| sem.wait() catch |err| {
                fatal = err; // Canceled
                break;
            };
            group.spawn(loadFileSegment, .{ load_sem, data_dir, info, allocator, &refs[i], &results[i] }) catch |err| {
                if (load_sem) |sem| sem.post(); // no loader will release it
                fatal = err;
                break;
            };
        }
        group.wait() catch |err| {
            fatal = fatal orelse err; // Canceled if startup is aborted
        };
    }

    // Collect every ref that actually loaded so the errdefer above frees them on
    // failure; propagate the first error (spawn / cancel / per-segment).
    for (results, refs, infos) |res, ref, info| {
        if (res) |_| {
            file_list.appendAssumeCapacity(ref);
            file_commit_id = @max(file_commit_id, info.getLastCommitId());
            file_version = @max(file_version, info.effectiveVersion());
            if (info.version != null) external_versions = true;
        } else |err| {
            if (err != error.SegmentNotLoaded) fatal = fatal orelse err;
        }
    }
    if (fatal) |err| return err;

    // 2. Open the oplog and replay only the tail (versions > file_commit_id).
    var ctx = ReplayCtx{ .allocator = allocator, .mem_list = &mem_list, .file_commit_id = file_commit_id };
    var oplog = try Oplog.open(allocator, oplog_dir, sync, &ctx, ReplayCtx.apply);
    errdefer oplog.deinit();
    if (ctx.external_versions) external_versions = true;

    const commit_id = @max(file_commit_id, oplog.last_commit_id);
    // The resume point: the newest position durable anywhere, in file segments or in
    // the replayed WAL tail. Derived independently of the commit id, since the two
    // no longer move together.
    const version = @max(file_version, oplog.last_version);

    // The WAL can hold less than the index does: a bootstrap deletes it outright and
    // restores segments carrying the donor's commit ids. Seed it from the recovered
    // maxima, or the next append would mint commit id 1 on top of existing segments —
    // breaking the dense tiling SegmentInfo.merge asserts on, and colliding with
    // segment file names, which are built from the commit id.
    oplog.last_commit_id = commit_id;
    oplog.last_version = version;

    const segments = try SharedPtr(Segments).create(allocator, .{
        .allocator = allocator,
        .file = try file_list.toOwnedSlice(allocator),
        .memory = try mem_list.toOwnedSlice(allocator),
        .commit_id = commit_id,
        .file_commit_id = file_commit_id,
        .version = version,
        .file_version = file_version,
        .external_versions = external_versions,
    });

    return .{
        .allocator = allocator,
        .dir = dir,
        .data_dir = data_dir,
        .oplog_dir = oplog_dir,
        .oplog = oplog,
        .segments = segments,
        .commit_id = commit_id,
        .file_commit_id = file_commit_id,
        .version = version,
        .file_version = file_version,
        .external_versions = external_versions,
        .checkpoint_threshold = checkpoint_threshold,
    };
}

// Group-spawned; captures its result into out_res (void return keeps the group's
// error flags clean — errors are collected by the caller instead) and releases
// the load permit the spawner acquired.
fn loadFileSegment(load_sem: ?*zio.Semaphore, dir: zio.Dir, info: SegmentInfo, allocator: std.mem.Allocator, out_ref: *FileRef, out_res: *anyerror!void) void {
    defer if (load_sem) |sem| sem.post();
    out_res.* = loadFileSegmentInner(dir, info, allocator, out_ref);
}

fn loadFileSegmentInner(dir: zio.Dir, info: SegmentInfo, allocator: std.mem.Allocator, out_ref: *FileRef) !void {
    var ref = try FileRef.create(allocator, FileSegment.init(allocator));
    errdefer ref.release(allocator, FileSegment.deinit, .{});
    try filefmt.readSegment(dir, info, ref.value);
    out_ref.* = ref;
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
    file_commit_id: u64,
    external_versions: bool = false,

    fn apply(self: *ReplayCtx, txn: Transaction) !void {
        // Before the filter below: a checkpointed transaction still tells us this
        // index has an upstream, even though its data already sits in a segment.
        if (txn.version != null) self.external_versions = true;
        if (txn.id <= self.file_commit_id) return; // already in a file segment
        var ref = try MemoryRef.create(self.allocator, MemorySegment.init(self.allocator, .{}));
        errdefer ref.release(self.allocator, MemorySegment.deinit, .{});
        try ref.value.build(txn.changes);
        ref.value.info = .{ .commit_id = txn.id, .merges = 0, .version = txn.version };
        try self.mem_list.append(self.allocator, ref);
    }
};

pub fn deinit(self: *Self) void {
    self.stop();
    self.oplog.deinit();
    self.segments.release(self.allocator, Segments.deinit, .{});
    self.oplog_dir.close();
    self.data_dir.close();
    self.dir.close();
}

/// Acquire a consistent snapshot to search. Caller must deinit it.
pub fn acquireReader(self: *Self) !IndexReader {
    try self.segments_lock.lockShared();
    defer self.segments_lock.unlockShared();
    return .{ .allocator = self.allocator, .snapshot = self.segments.acquire() };
}

// Install a new snapshot, swap it in, and release the old one (dropping the
// files of any segment no longer referenced). Takes ownership of file/memory on
// success; on any error (cancelled lock or OOM) leaves them for the caller.
// Allocate the new segment snapshot. This is the ONLY fallible step of a commit, so
// callers do it BEFORE the durable manifest write — an OOM here commits nothing. On
// success the snapshot OWNS `file` and `memory`; on failure it consumes neither, so
// the caller's errdefers release them.
const Watermarks = struct {
    commit_id: u64,
    file_commit_id: u64,
    version: u64,
    file_version: u64,
};

fn createSnapshot(self: *Self, file: []FileRef, memory: []MemoryRef, w: Watermarks) !SharedPtr(Segments) {
    return SharedPtr(Segments).create(self.allocator, .{
        .allocator = self.allocator,
        .file = file,
        .memory = memory,
        .commit_id = w.commit_id,
        .file_commit_id = w.file_commit_id,
        .version = w.version,
        .file_version = w.file_version,
        .external_versions = self.external_versions,
    });
}

// Swap a pre-built snapshot into place. Infallible — the allocation already happened
// in createSnapshot and the lock is uncancelable — so it runs AFTER the durable
// manifest write with no window where a committed manifest points at state we then
// fail to install (this mirrors the old commit_id's allocation-free commitUpdate).
// Releases the superseded snapshot, deleting any segment file whose last reference it
// held, and adopts the snapshot's commit_id bookkeeping. Consumes `snap`.
fn swapSnapshot(self: *Self, snap: SharedPtr(Segments)) void {
    const commit_id = snap.value.commit_id;
    const file_commit_id = snap.value.file_commit_id;
    const version = snap.value.version;
    const file_version = snap.value.file_version;

    self.segments_lock.lockUncancelable();
    var old = self.segments;
    self.segments = snap;
    self.segments_lock.unlock();

    old.release(self.allocator, Segments.deinit, .{});
    self.commit_id = commit_id;
    self.file_commit_id = file_commit_id;
    self.version = version;
    self.file_version = file_version;
}

// Collapse runs of equal values in a sorted slice in place; returns the unique
// prefix.
fn dedupSorted(sorted: []u32) []u32 {
    if (sorted.len == 0) return sorted;
    var w: usize = 1;
    for (sorted[1..]) |v| {
        if (v != sorted[w - 1]) {
            sorted[w] = v;
            w += 1;
        }
    }
    return sorted[0..w];
}

fn cloneRefs(comptime T: type, allocator: std.mem.Allocator, src: []SharedPtr(T)) ![]SharedPtr(T) {
    const dst = try allocator.alloc(SharedPtr(T), src.len);
    for (src, 0..) |s, i| dst[i] = s.acquire();
    return dst;
}

fn releaseRefs(comptime T: type, allocator: std.mem.Allocator, refs: []SharedPtr(T)) void {
    for (refs) |*s| s.release(allocator, T.deinit, .{});
    allocator.free(refs);
}

// Writer path. Build the memory segment before the durable append so a build
// failure never leaves the log ahead of memory; the oplog append is the commit
// point.
pub fn update(self: *Self, changes: []const Change, options: Oplog.WriteOptions) !u64 {
    try self.write_lock.lock();
    defer self.write_lock.unlock();

    // Shield the commit: once we hold the write lock, the oplog append / manifest
    // write / snapshot swap must complete atomically even under cancellation.
    zio.beginShield();
    defer zio.endShield();

    // Once anything in this index carries an upstream position, every later commit
    // needs one. Minting a local version on top of upstream ones would invent a
    // position the upstream never issued — and this node advertises that number as a
    // watermark, so a peer bootstrapping from it would resume the feed at a position
    // that does not exist and skip whatever really sits there.
    if (self.external_versions and options.version == null) return error.VersionRequired;

    var seg = try MemoryRef.create(self.allocator, MemorySegment.init(self.allocator, .{}));
    var seg_consumed = false;
    errdefer if (!seg_consumed) seg.release(self.allocator, MemorySegment.deinit, .{});
    try seg.value.build(changes);

    const cur = self.segments.value;
    const new_file = try cloneRefs(FileSegment, self.allocator, cur.file);
    var arrays_consumed = false;
    errdefer if (!arrays_consumed) releaseRefs(FileSegment, self.allocator, new_file);

    const new_memory = try self.allocator.alloc(MemoryRef, cur.memory.len + 1);
    // On a later failure new_memory is fully populated (the fill below is
    // allocation-free), so release every ref — not just free the array. The clones
    // are shared (their .delete is a no-op until their real last drop); `seg` is the
    // uncommitted new segment and is deleted.
    errdefer if (!arrays_consumed) {
        for (new_memory) |*s| s.release(self.allocator, MemorySegment.deinit, .{});
        self.allocator.free(new_memory);
    };
    for (cur.memory, 0..) |s, i| new_memory[i] = s.acquire();
    new_memory[cur.memory.len] = seg;
    seg_consumed = true;

    var snap = try self.createSnapshot(new_file, new_memory, .{
        .commit_id = self.commit_id,
        .file_commit_id = self.file_commit_id,
        .version = self.version,
        .file_version = self.file_version,
    });
    arrays_consumed = true;
    var snap_consumed = false;
    errdefer if (!snap_consumed) snap.release(self.allocator, Segments.deinit, .{});

    // Everything needed to publish the new state is allocated before the durable
    // append. After this succeeds, only field assignments and the infallible snapshot
    // swap remain, so an allocation failure cannot leave the WAL ahead of memory.
    const commit = try self.oplog.append(changes, options);
    // `options.version`, not the resolved one: a local commit must leave this null, or
    // the segment would look upstream-fed and poison an index that never had a feed.
    seg.value.info = .{ .commit_id = commit.commit_id, .merges = 0, .version = options.version };
    snap.value.commit_id = commit.commit_id;
    snap.value.version = commit.version;
    snap.value.external_versions = self.external_versions or options.version != null;

    self.swapSnapshot(snap);
    snap_consumed = true;

    // Any update may create maintenance work (memory merge, then checkpoint,
    // then file merge). Signal the coroutine; it decides what's actually needed.
    // Level-triggered, so it's safe even when the coroutine isn't running (tests
    // drive runMaintenance() synchronously instead).
    if (options.version != null) self.external_versions = true;
    self.wake.set();
    // The version, not the commit id: this is what callers see (an _update response, a
    // read-your-writes wait) and it must be comparable with positions from the feed.
    return commit.version;
}

/// Start the background maintenance coroutine. Call once the Index is at its
/// final address (the coroutine captures `self`).
pub fn start(self: *Self) !void {
    self.maintenance = try zio.spawn(maintenanceLoop, .{self});
}

/// Stop the maintenance coroutine and wait for it to finish. Call before deinit.
/// Cancellation aborts an in-progress merge (its commit is shielded, so it
/// either completes atomically or hasn't started); cancel() also joins.
pub fn stop(self: *Self) void {
    if (self.maintenance) |*task| {
        task.cancel();
        self.maintenance = null;
    }
}

// Runs until cancelled. Cancellation surfaces as error.Canceled from wait() or
// from an in-progress merge, and is propagated out (the task result). Other
// (transient) errors are logged and the loop keeps going.
fn maintenanceLoop(self: *Self) zio.Cancelable!void {
    while (true) {
        // Block for the next update; when age-based checkpointing is on, also wake on
        // a timer so a pending batch gets flushed even without new writes (the age
        // trigger then fires within [age, 2*age] of the first write).
        if (self.checkpoint_age) |age| {
            self.wake.timedWait(.{ .duration = age }) catch |err| switch (err) {
                error.Timeout => {},
                error.Canceled => return error.Canceled,
            };
        } else {
            try self.wake.wait();
        }
        self.wake.reset(); // reset before processing so a set() during the pass isn't lost
        self.runMaintenance() catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            else => log.warn("maintenance failed: {}", .{err}),
        };
    }
}

// Cascade all pending work until nothing is left: consolidate memory segments,
// flush memory to a file segment, then merge file segments. A concurrent update
// may add work again, which re-signals the wake flag.
pub fn runMaintenance(self: *Self) !void {
    while (true) {
        if (try self.mergeMemory()) continue;
        if (try self.checkpoint(false)) continue;
        if (try self.mergeFiles()) continue;
        break;
    }
}

/// Flush every memory segment to a file segment regardless of threshold or age, so
/// everything the index holds is durable on disk. A bootstrap build needs this before
/// its staging directory is swapped in: the swap path reopens from disk alone, and
/// whatever only lived in memory would silently vanish from the installed index.
pub fn flush(self: *Self) !void {
    while (try self.mergeMemory()) {}
    _ = try self.checkpoint(true);
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

    try self.segments_lock.lockShared();
    var snap = self.segments.acquire();
    self.segments_lock.unlockShared();
    // Once this op commits, `snap` is the superseded snapshot; if no reader still
    // holds it, this drop retires it. Any file segment the merge marked
    // delete_on_destroy is deleted here on its last reference.
    defer snap.release(self.allocator, Segments.deinit, .{});

    const src_mem = snap.value.memory;
    if (src_mem.len <= policy.calculateBudget(src_mem)) return false;
    const candidate = policy.findSegmentsToMerge(src_mem) orelse return false;
    const lo = candidate.start;
    const hi = candidate.end;
    if (hi - lo < 2) return false;
    const n = hi - lo;

    var merged = try MemoryRef.create(self.allocator, MemorySegment.init(self.allocator, .{}));
    var merged_placed = false;
    var committed = false;
    var arrays_consumed = false;
    errdefer if (!merged_placed) merged.release(self.allocator, MemorySegment.deinit, .{});
    {
        var merger = try SegmentMerger(MemorySegment).init(self.allocator, n);
        defer merger.deinit();
        for (src_mem[lo..hi]) |s| merger.addSource(s.value);
        try merger.prepare(snap.value);
        try merged.value.buildFromMerger(&merger);
    }

    try self.write_lock.lock();
    defer self.write_lock.unlock();

    // Shield the commit: once we hold the write lock, the oplog append / manifest
    // write / snapshot swap must complete atomically even under cancellation.
    zio.beginShield();
    defer zio.endShield();

    // Existing memory segments don't move (updates only append), so lo/hi stay
    // valid; the suffix picks up updates that arrived during the merge.
    const cur = self.segments.value;
    const new_memory = try self.allocator.alloc(MemoryRef, cur.memory.len - n + 1);
    var nm: usize = 0;
    errdefer if (!arrays_consumed) {
        for (new_memory[0..nm]) |*s| s.release(self.allocator, MemorySegment.deinit, .{});
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
    errdefer if (!arrays_consumed) releaseRefs(FileSegment, self.allocator, new_file);

    var new_snap = try self.createSnapshot(new_file, new_memory, .{
        .commit_id = self.commit_id,
        .file_commit_id = self.file_commit_id,
        .version = self.version,
        .file_version = self.file_version,
    });
    arrays_consumed = true;
    errdefer if (!committed) new_snap.release(self.allocator, Segments.deinit, .{});
    self.swapSnapshot(new_snap);
    committed = true;

    metrics.incMemoryMerges();
    log.info("merged {} memory segments -> {x}-{x} ({} items)", .{ n, merged.value.info.commit_id, merged.value.info.merges, merged.value.getSize() });
    return true;
}

// Flush all memory segments to one file segment. The merge runs without the
// write lock; only the manifest write + snapshot swap hold it, so updates keep
// flowing. Updates that arrive during the merge stay in memory (they append to
// the suffix; the flushed segments are the prefix). Returns true if it ran.
fn checkpoint(self: *Self, force: bool) !bool {
    try self.segments_lock.lockShared();
    var snap = self.segments.acquire();
    self.segments_lock.unlockShared();
    // Once this op commits, `snap` is the superseded snapshot; if no reader still
    // holds it, this drop retires it. Any file segment the merge marked
    // delete_on_destroy is deleted here on its last reference.
    defer snap.release(self.allocator, Segments.deinit, .{});

    const flush_count = snap.value.memory.len;
    if (flush_count == 0) {
        self.pending_since = null; // nothing pending
        return false;
    }
    // Start the age clock the first time maintenance sees this uncheckpointed batch.
    if (self.pending_since == null) self.pending_since = zio.Timestamp.now(.monotonic);

    // Flush when memory is large enough, or when the oldest pending write has aged out.
    const over_threshold = memorySize(snap.value.memory) > self.checkpoint_threshold;
    const aged = if (self.checkpoint_age) |age|
        self.pending_since.?.untilNow(.monotonic).toNanoseconds() >= age.toNanoseconds()
    else
        false;
    if (!force and !over_threshold and !aged) return false;

    var fseg = try self.mergeToFileSegment(MemorySegment, snap.value.memory, snap.value);
    var committed = false;
    var arrays_consumed = false;
    var fseg_placed = false;
    errdefer if (!fseg_placed) fseg.release(self.allocator, FileSegment.deinit, .{});
    const info = fseg.value.info;

    try self.write_lock.lock();
    defer self.write_lock.unlock();

    // Shield the commit: once we hold the write lock, the oplog append / manifest
    // write / snapshot swap must complete atomically even under cancellation.
    zio.beginShield();
    defer zio.endShield();

    const cur = self.segments.value;
    const kept = cur.memory[flush_count..];

    const new_file = try self.allocator.alloc(FileRef, cur.file.len + 1);
    var nf: usize = 0;
    errdefer if (!arrays_consumed) {
        for (new_file[0..nf]) |*s| s.release(self.allocator, FileSegment.deinit, .{});
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
    errdefer if (!arrays_consumed) releaseRefs(MemorySegment, self.allocator, new_memory);

    // Allocate the snapshot BEFORE the durable manifest write, so the manifest is the
    // last fallible step and the swap after it can't fail (no window where a committed
    // manifest points at state we then fail to install).
    var new_snap = try self.createSnapshot(new_file, new_memory, .{
        .commit_id = self.commit_id,
        .file_commit_id = @max(self.file_commit_id, info.getLastCommitId()),
        .version = self.version,
        // The donor watermark: a snapshot taken now carries this segment, so a
        // fetcher resumes from the position it covers.
        .file_version = @max(self.file_version, info.effectiveVersion()),
    });
    arrays_consumed = true;
    errdefer if (!committed) new_snap.release(self.allocator, Segments.deinit, .{});

    try self.writeManifestFor(new_snap.value.file);
    self.swapSnapshot(new_snap);
    committed = true;

    // Restart the age clock: cleared if nothing is left pending, else stamped now for
    // the segments that arrived during the flush (their real age is ~the flush time).
    self.pending_since = if (kept.len == 0) null else zio.Timestamp.now(.monotonic);

    // Transactions up to file_commit_id are now durable in file segments; drop the
    // oplog files entirely below it. (After the manifest commit, so a crash in
    // between just leaves redundant oplog entries that replay skips.)
    self.oplog.truncate(self.file_commit_id) catch |err| {
        log.warn("oplog truncate failed: {}", .{err});
    };

    metrics.incCheckpoints();
    log.info("checkpointed to file segment {x}-{x} ({} items)", .{ info.commit_id, info.merges, fseg.value.num_items });
    return true;
}

// Merge a tiered-policy-selected range of file segments into one. Same phase
// split: the merge is lock-free, only the swap holds the write lock. The
// merged-away segments are marked delete_on_destroy after the commit, so their
// files are deleted when the last snapshot/reader referencing them drops (an
// in-flight reader keeps them until done). Returns true if a merge ran.
fn mergeFiles(self: *Self) !bool {
    const policy = TieredMergePolicy(FileRef, fileSegmentSize, fileSegmentFrozen){
        .min_segment_size = 100,
        .max_segment_size = 1_000_000_000,
        .segments_per_merge = 10,
        .segments_per_level = 10,
    };

    try self.segments_lock.lockShared();
    var snap = self.segments.acquire();
    self.segments_lock.unlockShared();
    // Once this op commits, `snap` is the superseded snapshot; if no reader still
    // holds it, this drop retires it. Any file segment the merge marked
    // delete_on_destroy is deleted here on its last reference.
    defer snap.release(self.allocator, Segments.deinit, .{});

    const src_file = snap.value.file;
    if (src_file.len <= policy.calculateBudget(src_file)) return false;
    const candidate = policy.findSegmentsToMerge(src_file) orelse return false;
    const lo = candidate.start;
    const hi = candidate.end;
    if (hi - lo < 2) return false;
    const n = hi - lo;

    var fseg = try self.mergeToFileSegment(FileSegment, src_file[lo..hi], snap.value);
    var committed = false;
    var arrays_consumed = false;
    var fseg_placed = false;
    errdefer if (!fseg_placed) fseg.release(self.allocator, FileSegment.deinit, .{});
    const info = fseg.value.info;

    try self.write_lock.lock();
    defer self.write_lock.unlock();

    // Shield the commit: once we hold the write lock, the oplog append / manifest
    // write / snapshot swap must complete atomically even under cancellation.
    zio.beginShield();
    defer zio.endShield();

    // File segments are unchanged since the capture (only this coroutine touches
    // them), so start/end are still valid; memory may have grown.
    const cur = self.segments.value;
    const new_file = try self.allocator.alloc(FileRef, cur.file.len - n + 1);
    var nf: usize = 0;
    errdefer if (!arrays_consumed) {
        for (new_file[0..nf]) |*s| s.release(self.allocator, FileSegment.deinit, .{});
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
    errdefer if (!arrays_consumed) releaseRefs(MemorySegment, self.allocator, new_memory);

    // Allocate the snapshot BEFORE the durable manifest write (see checkpoint), so the
    // swap after the manifest is infallible.
    var new_snap = try self.createSnapshot(new_file, new_memory, .{
        .commit_id = self.commit_id,
        .file_commit_id = self.file_commit_id,
        .version = self.version,
        .file_version = self.file_version,
    });
    arrays_consumed = true;
    errdefer if (!committed) new_snap.release(self.allocator, Segments.deinit, .{});

    try self.writeManifestFor(new_snap.value.file);
    self.swapSnapshot(new_snap);
    committed = true;

    // Retire the merged-away segments: mark them so their backing files are deleted
    // when the last snapshot/reader referencing them drops. Done AFTER the commit, so
    // a failed/rolled-back merge never marks a still-live segment for deletion. This
    // is the ONLY place file segments are retired (checkpoint only adds one).
    for (cur.file[lo..hi]) |s| s.value.delete_on_destroy = true;

    metrics.incFileMerges();
    log.info("merged {} file segments -> {x}-{x} ({} items)", .{ n, info.commit_id, info.merges, fseg.value.num_items });
    return true;
}

// Merge `sources` into a new on-disk file segment and load it back. `collection`
// provides hasNewerCommit (the current snapshot). On error the written file is
// removed. Caller owns the returned ref.
fn mergeToFileSegment(self: *Self, comptime Segment: type, sources: []SharedPtr(Segment), collection: anytype) !FileRef {
    var merger = try SegmentMerger(Segment).init(self.allocator, sources.len);
    defer merger.deinit();
    for (sources) |s| merger.addSource(s.value);
    try merger.prepare(collection);
    const info = merger.segment.info;

    try filefmt.writeSegment(self.data_dir, &merger, self.allocator);
    errdefer {
        // Shielded so the just-written file is removed even under cancellation
        // (a cancelled task's I/O is otherwise skipped, orphaning the file).
        zio.beginShield();
        defer zio.endShield();
        filefmt.deleteSegmentFile(self.data_dir, info) catch |err| {
            log.warn("failed to remove segment file after error: {}", .{err});
        };
    }

    var ref = try FileRef.create(self.allocator, FileSegment.init(self.allocator));
    errdefer ref.release(self.allocator, FileSegment.deinit, .{});
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

// Count on-disk .data segment files under an index dir's data/ subdir (tests).
fn countDataFiles(cwd: zio.Dir, dir_path: []const u8) !usize {
    var buf: [256]u8 = undefined;
    const p = try std.fmt.bufPrint(&buf, "{s}/data", .{dir_path});
    var d = try cwd.openDir(p, .{ .iterate = true });
    defer d.close();
    var count: usize = 0;
    var it = d.iterate();
    while (try it.next()) |e| {
        if (e.kind == .file and std.mem.endsWith(u8, e.name, ".data")) count += 1;
    }
    return count;
}

test "allocation failure cannot leave the WAL ahead of published segments" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_index_update_oom";
    cleanupTestDir(cwd, dir_path);
    defer cleanupTestDir(cwd, dir_path);

    var saw_success = false;
    var fail_index: usize = 0;
    while (fail_index < 64 and !saw_success) : (fail_index += 1) {
        cleanupTestDir(cwd, dir_path);
        try cwd.createDir(dir_path, 0o755);

        var update_succeeded = false;
        {
            const dir = try cwd.openDir(dir_path, .{ .iterate = true });
            var index = try Self.open(std.testing.allocator, dir, 100_000, true, null);
            defer index.deinit();

            var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = fail_index });
            const allocator = failing.allocator();
            index.allocator = allocator;
            index.oplog.allocator = allocator;

            if (index.update(&[_]Change{.{ .insert = .{ .id = 1, .hashes = &[_]u32{ 10, 20 } } }}, .{})) |_| {
                update_succeeded = true;
            } else |_| {
                try std.testing.expect(failing.has_induced_failure);
            }
        }

        // An update that reported failure must not appear after replay. Once every
        // allocation succeeds, the same update must be durable and visible.
        {
            const dir = try cwd.openDir(dir_path, .{ .iterate = true });
            var index = try Self.open(std.testing.allocator, dir, 100_000, true, null);
            defer index.deinit();
            try std.testing.expectEqual(@as(u64, if (update_succeeded) 1 else 0), index.commit_id);
        }
        saw_success = update_succeeded;
    }
    try std.testing.expect(saw_success);
}

test "duplicate query hashes score consistently across memory and file segments" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_index_dup_query";
    cleanupTestDir(cwd, dir_path);
    try cwd.createDir(dir_path, 0o755);
    defer cleanupTestDir(cwd, dir_path);

    const dir = try cwd.openDir(dir_path, .{ .iterate = true });
    var index = try Self.open(std.testing.allocator, dir, 1, true, null); // threshold 1 -> flushes
    defer index.deinit();

    _ = try index.update(&[_]Change{.{ .insert = .{ .id = 1, .hashes = &[_]u32{ 100, 200 } } }}, .{});

    // In-memory: a repeated query hash scores the doc once (query is a set).
    {
        var r = SearchResults.init(std.testing.allocator, .{ .max_results = 10, .min_score = 1 });
        defer r.deinit();
        var reader = try index.acquireReader();
        defer reader.deinit();
        var q = [_]u32{ 100, 100 };
        try reader.search(&q, &r);
        try std.testing.expectEqual(@as(u32, 1), r.hits.get(1).?.score);
    }

    // After flushing to a file segment the same query must score identically (it
    // scored 2 before the dedupe, because FileSegment re-probed the repeated hash).
    try index.runMaintenance();
    try std.testing.expectEqual(@as(usize, 1), index.segments.value.file.len);
    {
        var r = SearchResults.init(std.testing.allocator, .{ .max_results = 10, .min_score = 1 });
        defer r.deinit();
        var reader = try index.acquireReader();
        defer reader.deinit();
        var q = [_]u32{ 100, 100 };
        try reader.search(&q, &r);
        try std.testing.expectEqual(@as(u32, 1), r.hits.get(1).?.score);
    }
}

test "age-based checkpoint flushes a small pending batch" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_index_age_checkpoint";
    cleanupTestDir(cwd, dir_path);
    try cwd.createDir(dir_path, 0o755);
    defer cleanupTestDir(cwd, dir_path);

    const dir = try cwd.openDir(dir_path, .{ .iterate = true });
    // High size threshold so only the age trigger can flush.
    var index = try Self.open(std.testing.allocator, dir, 100_000, true, null);
    index.checkpoint_age = .fromMilliseconds(50);
    defer index.deinit();
    try index.start(); // run the background maintenance loop (the age timer lives here)

    _ = try index.update(&[_]Change{.{ .insert = .{ .id = 1, .hashes = &[_]u32{ 100, 200 } } }}, .{});

    // The tiny batch is far below the size threshold, so only the aged-out trigger
    // flushes it (within ~2*age). Read the file count via a reader — the maintenance
    // coroutine swaps segments concurrently.
    var i: usize = 0;
    const flushed = while (i < 100) : (i += 1) {
        var r = try index.acquireReader();
        const file_len = r.snapshot.value.file.len;
        r.deinit();
        if (file_len == 1) break true;
        try zio.sleep(.fromMilliseconds(20));
    } else false;
    try std.testing.expect(flushed);

    var r = try index.acquireReader();
    defer r.deinit();
    try std.testing.expectEqual(@as(usize, 0), r.snapshot.value.memory.len);
}

test "flush checkpoints a batch the threshold and age triggers would both skip" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_index_flush";
    cleanupTestDir(cwd, dir_path);
    try cwd.createDir(dir_path, 0o755);
    defer cleanupTestDir(cwd, dir_path);

    const dir = try cwd.openDir(dir_path, .{ .iterate = true });
    // High size threshold, no age trigger: nothing but flush() can checkpoint this.
    var index = try Self.open(std.testing.allocator, dir, 100_000, true, null);
    defer index.deinit();

    _ = try index.update(&[_]Change{.{ .insert = .{ .id = 1, .hashes = &[_]u32{ 100, 200 } } }}, .{ .version = 7 });
    try index.flush();

    var r = try index.acquireReader();
    defer r.deinit();
    try std.testing.expectEqual(@as(usize, 0), r.snapshot.value.memory.len);
    try std.testing.expectEqual(@as(usize, 1), r.snapshot.value.file.len);
    // The upstream position is now durable in a file segment — what a bootstrap
    // install (which reopens from disk alone) resumes the feed from.
    try std.testing.expectEqual(@as(u64, 7), r.snapshot.value.file_version);
}

test "snapshot archive round-trips manifest + file segments" {
    const snapshot = @import("snapshot.zig");
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_index_snapshot";
    cleanupTestDir(cwd, dir_path);
    try cwd.createDir(dir_path, 0o755);
    defer cleanupTestDir(cwd, dir_path);

    const dir = try cwd.openDir(dir_path, .{ .iterate = true });
    var index = try Self.open(std.testing.allocator, dir, 1, true, null); // threshold 1 -> flush each
    defer index.deinit();

    _ = try index.update(&[_]Change{.{ .insert = .{ .id = 1, .hashes = &[_]u32{ 100, 200, 300 } } }}, .{});
    try index.runMaintenance();
    _ = try index.update(&[_]Change{.{ .insert = .{ .id = 2, .hashes = &[_]u32{ 150, 250 } } }}, .{});
    try index.runMaintenance();

    var reader = try index.acquireReader();
    defer reader.deinit();
    const segs = reader.snapshot.value;
    try std.testing.expect(segs.file.len >= 1);

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var buf: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer buf.deinit();
    try snapshot.writeSnapshot(&buf.writer, arena.allocator(), segs, 7);

    const parsed = try snapshot.parse(arena.allocator(), buf.written());

    // generation preserved, one entry per file segment (the manifest is reconstructed
    // from the entries' infos, not shipped), and every segment's bytes survive verbatim
    // (so a restore reproduces the file exactly).
    try std.testing.expectEqual(@as(u64, 7), parsed.generation);
    try std.testing.expectEqual(segs.file.len, parsed.entries.len);
    for (segs.file, 0..) |s, i| {
        try std.testing.expectEqual(s.value.info.commit_id, parsed.entries[i].info.commit_id);
        try std.testing.expectEqual(s.value.info.merges, parsed.entries[i].info.merges);
        try std.testing.expectEqualSlices(u8, s.value.data, parsed.entries[i].data);
    }
}

test "snapshot restores into a fresh dir and opens at the same commit_id" {
    const snapshot = @import("snapshot.zig");
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const src_path = "test_snap_restore_src";
    const dst_path = "test_snap_restore_dst";
    cleanupTestDir(cwd, src_path);
    cleanupTestDir(cwd, dst_path);
    try cwd.createDir(src_path, 0o755);
    try cwd.createDir(dst_path, 0o755);
    defer cleanupTestDir(cwd, src_path);
    defer cleanupTestDir(cwd, dst_path);

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Source with two file segments (threshold 1 -> checkpoint each update).
    const src_dir = try cwd.openDir(src_path, .{ .iterate = true });
    var src = try Self.open(std.testing.allocator, src_dir, 1, true, null);
    _ = try src.update(&[_]Change{.{ .insert = .{ .id = 1, .hashes = &[_]u32{ 100, 200, 300 } } }}, .{});
    try src.runMaintenance();
    _ = try src.update(&[_]Change{.{ .insert = .{ .id = 2, .hashes = &[_]u32{ 400, 500 } } }}, .{});
    try src.runMaintenance();

    var reader = try src.acquireReader();
    const src_version = reader.snapshot.value.commit_id;
    var buf: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer buf.deinit();
    try snapshot.writeSnapshot(&buf.writer, a, reader.snapshot.value, 7);
    reader.deinit();
    src.deinit();

    const dst_dir = try cwd.openDir(dst_path, .{ .iterate = true });

    // The manifest + segments go into the index's "data" subdir (Index.open reads them
    // from there; the "oplog" WAL subdir is created empty on open).
    const dst_data = try openOrCreateDir(dst_dir, "data");

    // Wrong generation is rejected (and writes nothing — the check precedes any write).
    var rw = std.Io.Reader.fixed(buf.written());
    try std.testing.expectError(error.SnapshotGenerationMismatch, snapshot.restoreInto(dst_data, &rw, a, 8));

    // Restore the right generation, then open: same commit_id, data searchable.
    var rr = std.Io.Reader.fixed(buf.written());
    try snapshot.restoreInto(dst_data, &rr, a, 7);
    dst_data.close();

    var dst = try Self.open(std.testing.allocator, dst_dir, 100_000, true, null);
    defer dst.deinit();
    try std.testing.expectEqual(src_version, dst.commit_id);

    var results = SearchResults.init(a, .{});
    defer results.deinit();
    var dr = try dst.acquireReader();
    defer dr.deinit();
    var q = [_]u32{ 100, 200, 300 };
    try dr.search(&q, &results);
    const hits = results.getResults();
    try std.testing.expectEqual(@as(usize, 1), hits.len);
    try std.testing.expectEqual(@as(u32, 1), hits[0].id);
}

test "merged-away files are deleted even when a reader outlived the merge" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_index_reader_merge_cleanup";
    cleanupTestDir(cwd, dir_path);
    try cwd.createDir(dir_path, 0o755);
    defer cleanupTestDir(cwd, dir_path);

    const dir = try cwd.openDir(dir_path, .{ .iterate = true });
    var index = try Self.open(std.testing.allocator, dir, 1, true, null); // threshold 1 -> checkpoint each
    defer index.deinit();

    // Build several file segments, then pin the snapshot with a reader.
    var id: u32 = 1;
    while (id <= 8) : (id += 1) {
        _ = try index.update(&[_]Change{.{ .insert = .{ .id = id, .hashes = &[_]u32{ 100, id } } }}, .{});
        try index.runMaintenance();
    }
    var reader = try index.acquireReader();

    // More updates + maintenance: file merges supersede the reader's snapshot and
    // merge away segments it still references.
    while (id <= 40) : (id += 1) {
        _ = try index.update(&[_]Change{.{ .insert = .{ .id = id, .hashes = &[_]u32{ 100, id } } }}, .{});
        try index.runMaintenance();
    }

    // Merged-away files the reader pins are still on disk (more than the live set).
    const pinned = try countDataFiles(cwd, dir_path);
    try std.testing.expect(pinned > index.segments.value.file.len);

    // Releasing the reader deletes them: on-disk .data == the live file-segment count.
    reader.deinit();
    try std.testing.expectEqual(index.segments.value.file.len, try countDataFiles(cwd, dir_path));
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
        var index = try Self.open(std.testing.allocator, dir, 5, true, null);
        defer index.deinit();

        _ = try index.update(&ins1, .{});
        _ = try index.update(&ins2, .{});
        try index.runMaintenance(); // flush memory -> file segment

        try std.testing.expectEqual(@as(usize, 1), index.segments.value.file.len);
        try std.testing.expectEqual(@as(usize, 0), index.segments.value.memory.len);

        var results = SearchResults.init(std.testing.allocator, .{ .max_results = 10, .min_score = 1 });
        defer results.deinit();
        var reader = try index.acquireReader();
        defer reader.deinit();
        var hashes = [_]u32{ 100, 200, 300 };
        try reader.search(&hashes, &results);
        try std.testing.expectEqual(@as(u32, 3), results.hits.get(1).?.score);
        try std.testing.expectEqual(@as(u32, 3), results.hits.get(2).?.score);
    }

    {
        const dir = try cwd.openDir(dir_path, .{ .iterate = true });
        var index = try Self.open(std.testing.allocator, dir, 5, true, null);
        defer index.deinit();

        try std.testing.expectEqual(@as(usize, 1), index.segments.value.file.len);
        try std.testing.expectEqual(@as(usize, 0), index.segments.value.memory.len);
        try std.testing.expectEqual(@as(u64, 2), index.commit_id);

        var results = SearchResults.init(std.testing.allocator, .{ .max_results = 10, .min_score = 1 });
        defer results.deinit();
        var reader = try index.acquireReader();
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
    var index = try Self.open(std.testing.allocator, dir, 1, true, null);
    defer index.deinit();

    var id: u32 = 1;
    while (id <= 30) : (id += 1) {
        const ins = [_]Change{.{ .insert = .{ .id = id, .hashes = &[_]u32{ 100, id } } }};
        _ = try index.update(&ins, .{});
        try index.runMaintenance();
    }
    try std.testing.expect(index.segments.value.file.len < 30);

    const del = [_]Change{.{ .delete = .{ .id = 5 } }};
    _ = try index.update(&del, .{});

    var results = SearchResults.init(std.testing.allocator, .{ .max_results = 100, .min_score = 1 });
    defer results.deinit();
    var reader = try index.acquireReader();
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
    var index = try Self.open(std.testing.allocator, dir, 5, true, null);
    defer index.deinit();

    _ = try index.update(&[_]Change{.{ .insert = .{ .id = 1, .hashes = &[_]u32{100} } }}, .{});

    // Snapshot taken now sees only id 1.
    var reader = try index.acquireReader();
    defer reader.deinit();

    // Many more writes trigger checkpoints + merges that free the segments the
    // new snapshots no longer reference; the old reader must stay valid.
    var id: u32 = 2;
    while (id <= 30) : (id += 1) {
        _ = try index.update(&[_]Change{.{ .insert = .{ .id = id, .hashes = &[_]u32{100} } }}, .{});
        try index.runMaintenance();
    }

    var r1 = SearchResults.init(std.testing.allocator, .{ .max_results = 100, .min_score = 1 });
    defer r1.deinit();
    var h1 = [_]u32{100};
    try reader.search(&h1, &r1);
    try std.testing.expectEqual(@as(usize, 1), r1.getResults().len); // snapshot isolation

    var r2 = SearchResults.init(std.testing.allocator, .{ .max_results = 100, .min_score = 1 });
    defer r2.deinit();
    var fresh = try index.acquireReader();
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
    var index = try Self.open(std.testing.allocator, dir, 100_000, true, null); // high threshold: no checkpoint
    defer index.deinit();

    // 50 tiny updates stay well under the checkpoint threshold.
    var id: u32 = 1;
    while (id <= 50) : (id += 1) {
        _ = try index.update(&[_]Change{.{ .insert = .{ .id = id, .hashes = &[_]u32{id} } }}, .{});
        try index.runMaintenance();
    }

    // No checkpoint (below threshold), but memory merged into far fewer segments.
    try std.testing.expectEqual(@as(usize, 0), index.segments.value.file.len);
    try std.testing.expect(index.segments.value.memory.len < 50);

    // Everything still searchable.
    var results = SearchResults.init(std.testing.allocator, .{ .max_results = 100, .min_score = 1 });
    defer results.deinit();
    var reader = try index.acquireReader();
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
        var index = try Self.open(std.testing.allocator, dir, 10, true, null);
        defer index.deinit();
        index.oplog.max_file_size = 80; // force frequent rotation

        var id: u32 = 1;
        while (id <= 40) : (id += 1) {
            _ = try index.update(&[_]Change{.{ .insert = .{ .id = id, .hashes = &[_]u32{ 100, id } } }}, .{});
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
        var index = try Self.open(std.testing.allocator, dir, 10, true, null);
        defer index.deinit();

        var results = SearchResults.init(std.testing.allocator, .{ .max_results = 100, .min_score = 1 });
        defer results.deinit();
        var reader = try index.acquireReader();
        defer reader.deinit();
        var h = [_]u32{40}; // id 40's unique hash
        try reader.search(&h, &results);
        try std.testing.expectEqual(@as(usize, 1), results.getResults().len);
    }
}

test "getDocInfo reports version and tombstones, across a checkpoint" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_index_docinfo";
    cleanupTestDir(cwd, dir_path);
    try cwd.createDir(dir_path, 0o755);
    defer cleanupTestDir(cwd, dir_path);

    const dir = try cwd.openDir(dir_path, .{ .iterate = true });
    var index = try Self.open(std.testing.allocator, dir, 100_000, true, null);
    defer index.deinit();

    _ = try index.update(&[_]Change{.{ .insert = .{ .id = 5, .hashes = &[_]u32{ 10, 20 } } }}, .{});
    _ = try index.update(&[_]Change{.{ .insert = .{ .id = 6, .hashes = &[_]u32{ 10, 30 } } }}, .{});

    {
        var r = try index.acquireReader();
        defer r.deinit();
        try std.testing.expectEqual(@as(u64, 1), r.getDocInfo(5).?.version);
        try std.testing.expect(!r.getDocInfo(5).?.deleted);
        try std.testing.expectEqual(@as(u64, 2), r.getDocInfo(6).?.version);
        try std.testing.expect(r.getDocInfo(99) == null);
    }

    // Delete 5: newest segment wins, reported as a tombstone.
    _ = try index.update(&[_]Change{.{ .delete = .{ .id = 5 } }}, .{});
    {
        var r = try index.acquireReader();
        defer r.deinit();
        try std.testing.expect(r.getDocInfo(5).?.deleted);
        try std.testing.expectEqual(@as(u64, 3), r.getDocInfo(5).?.version);
    }

    // Same answers after flushing to a file segment.
    try index.runMaintenance();
    {
        var r = try index.acquireReader();
        defer r.deinit();
        try std.testing.expect(r.getDocInfo(5).?.deleted);
        try std.testing.expect(!r.getDocInfo(6).?.deleted);
        try std.testing.expectEqual(@as(u64, 2), r.getDocInfo(6).?.version);
    }
}

test "index stats and metadata aggregation" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_index_stats";
    cleanupTestDir(cwd, dir_path);
    try cwd.createDir(dir_path, 0o755);
    defer cleanupTestDir(cwd, dir_path);

    const dir = try cwd.openDir(dir_path, .{ .iterate = true });
    var index = try Self.open(std.testing.allocator, dir, 100_000, true, null);
    defer index.deinit();

    _ = try index.update(&[_]Change{
        .{ .insert = .{ .id = 5, .hashes = &[_]u32{ 10, 20 } } },
        .{ .set_metadata = .{ .entries = &[_]MetadataEntry{
            .{ .key = "source", .value = "unit" },
            .{ .key = "rev", .value = "1" },
        } } },
    }, .{});

    _ = try index.update(&[_]Change{
        .{ .insert = .{ .id = 12, .hashes = &[_]u32{30} } },
        .{ .set_metadata = .{ .entries = &[_]MetadataEntry{.{ .key = "rev", .value = "2" }} } },
    }, .{});

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var reader = try index.acquireReader();
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 2), reader.numSegments());
    try std.testing.expectEqual(@as(u32, 2), reader.numDocs());
    try std.testing.expectEqual(@as(u32, 5), reader.minDocId());
    try std.testing.expectEqual(@as(u32, 12), reader.maxDocId());

    const md = try reader.buildMetadata(arena.allocator());
    try std.testing.expectEqualStrings("unit", md.get("source").?);
    try std.testing.expectEqualStrings("2", md.get("rev").?); // newest wins
}

test "oplog recovers the valid prefix from a corrupt tail" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_oplog_torn";
    cleanupTestDir(cwd, dir_path);
    try cwd.createDir(dir_path, 0o755);
    defer cleanupTestDir(cwd, dir_path);
    var torn_file_size: u64 = 0;

    // Write 5 clean records.
    {
        const dir = try cwd.openDir(dir_path, .{ .iterate = true });
        var index = try Self.open(std.testing.allocator, dir, 100_000, true, null);
        defer index.deinit();
        var id: u32 = 1;
        while (id <= 5) : (id += 1) {
            _ = try index.update(&[_]Change{.{ .insert = .{ .id = id, .hashes = &[_]u32{ 100, id } } }}, .{});
        }
        try std.testing.expectEqual(@as(u64, 5), index.commit_id);
    }

    // Append a full but CRC-invalid framed record to the tail (a crash could leave
    // exactly this: a complete-looking record whose bytes didn't all land).
    {
        var oplog_dir = try cwd.openDir(dir_path ++ "/oplog", .{ .iterate = true });
        defer oplog_dir.close();
        const name = "0000000000000001.xlog"; // first record's commit_id = 1
        const st = try oplog_dir.statPath(name);
        const file = try oplog_dir.openFile(name, .{ .mode = .read_write });
        defer file.close();

        var junk: [12]u8 = undefined;
        std.mem.writeInt(u32, junk[0..4], 4, .little); // payload_len = 4
        std.mem.writeInt(u32, junk[4..8], 0xDEADBEEF, .little); // wrong crc
        @memcpy(junk[8..12], "junk");
        var written: usize = 0;
        while (written < junk.len) written += try file.write(junk[written..], @as(u64, @intCast(st.size)) + written);
        try file.sync(.{});
        torn_file_size = st.size + junk.len;
    }

    // Reopen, recover the valid prefix, and append another transaction.
    {
        const dir = try cwd.openDir(dir_path, .{ .iterate = true });
        var index = try Self.open(std.testing.allocator, dir, 100_000, true, null);
        defer index.deinit();
        try std.testing.expectEqual(@as(u64, 5), index.commit_id);

        _ = try index.update(&[_]Change{.{ .insert = .{ .id = 6, .hashes = &[_]u32{ 100, 6 } } }}, .{});
        try std.testing.expectEqual(@as(u64, 6), index.commit_id);

        const oplog_dir = try cwd.openDir(dir_path ++ "/oplog", .{ .iterate = true });
        defer oplog_dir.close();
        const st = try oplog_dir.statPath("0000000000000001.xlog");
        try std.testing.expectEqual(torn_file_size, st.size);
    }

    // The old torn tail no longer hides that append on the next restart.
    {
        const dir = try cwd.openDir(dir_path, .{ .iterate = true });
        var index = try Self.open(std.testing.allocator, dir, 100_000, true, null);
        defer index.deinit();
        try std.testing.expectEqual(@as(u64, 6), index.commit_id);

        var results = SearchResults.init(std.testing.allocator, .{ .max_results = 100, .min_score = 1 });
        defer results.deinit();
        var reader = try index.acquireReader();
        defer reader.deinit();
        var h = [_]u32{100};
        try reader.search(&h, &results);
        try std.testing.expectEqual(@as(usize, 6), results.getResults().len);
    }
}

test "coalesced batches keep commit ids dense while positions jump" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_index_coalesced";
    cleanupTestDir(cwd, dir_path);
    try cwd.createDir(dir_path, 0o755);
    defer cleanupTestDir(cwd, dir_path);

    const dir = try cwd.openDir(dir_path, .{ .iterate = true });
    var index = try Self.open(std.testing.allocator, dir, 100_000, true, null); // no checkpoint
    defer index.deinit();

    // A consumer coalesces a run of feed entries into one apply, so positions jump by
    // the batch size. Before commit ids and positions were separated this fed a
    // non-dense value into SegmentInfo.commit_id and merging tripped its adjacency
    // assertion (`commit_id + merges + 1 == other.commit_id`).
    var pos: u64 = 0;
    var id: u32 = 1;
    while (id <= 50) : (id += 1) {
        pos += 10;
        _ = try index.update(&[_]Change{.{ .insert = .{ .id = id, .hashes = &[_]u32{id} } }}, .{ .version = pos });
        try index.runMaintenance(); // merges memory segments
    }

    try std.testing.expect(index.segments.value.memory.len < 50); // merging happened
    try std.testing.expectEqual(@as(u64, 500), index.version); // external position
    try std.testing.expectEqual(@as(u64, 50), index.commit_id); // dense internal ids

    // The merged segments still tile the commit-id sequence without gaps.
    var expected: u64 = 1;
    for (index.segments.value.memory) |seg| {
        try std.testing.expectEqual(expected, seg.value.info.commit_id);
        expected = seg.value.info.getLastCommitId() + 1;
    }
    try std.testing.expectEqual(@as(u64, 51), expected);

    // And every doc is still searchable.
    var results = SearchResults.init(std.testing.allocator, .{ .max_results = 100, .min_score = 1 });
    defer results.deinit();
    var reader = try index.acquireReader();
    defer reader.deinit();
    var h = [_]u32{25};
    try reader.search(&h, &results);
    try std.testing.expectEqual(@as(usize, 1), results.getResults().len);
}

test "version survives checkpoint and restart, and drives the donor watermark" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_index_logpos_restart";
    cleanupTestDir(cwd, dir_path);
    try cwd.createDir(dir_path, 0o755);
    defer cleanupTestDir(cwd, dir_path);

    var checkpointed_at: u64 = 0;
    {
        const dir = try cwd.openDir(dir_path, .{ .iterate = true });
        var index = try Self.open(std.testing.allocator, dir, 10, true, null);
        defer index.deinit();

        // Write until something has been checkpointed into a file segment.
        var pos: u64 = 0;
        var id: u32 = 1;
        while (id <= 40) : (id += 1) {
            pos += 7; // positions jump; commit ids stay dense
            _ = try index.update(&[_]Change{.{ .insert = .{ .id = id, .hashes = &[_]u32{ 100, id } } }}, .{ .version = pos });
            try index.runMaintenance();
        }
        try std.testing.expect(index.segments.value.file.len > 0);

        // The donor watermark is a real position, and never ahead of the resume point.
        checkpointed_at = index.file_version;
        try std.testing.expect(checkpointed_at > 0);
        try std.testing.expect(checkpointed_at <= index.version);
        try std.testing.expectEqual(@as(u64, 280), index.version);
        try std.testing.expectEqual(@as(u64, 40), index.commit_id); // dense commit ids

        // A write that stays in memory moves the resume point but NOT the watermark a
        // snapshot would hand a fetcher — a snapshot carries file segments only.
        _ = try index.update(&[_]Change{.{ .insert = .{ .id = 99, .hashes = &[_]u32{999} } }}, .{ .version = 5000 });
        try std.testing.expectEqual(@as(u64, 5000), index.version);
        try std.testing.expectEqual(checkpointed_at, index.file_version);
    }

    // Reopen: the position comes back from the manifest plus the replayed WAL tail,
    // derived independently of the commit ids.
    {
        const dir = try cwd.openDir(dir_path, .{ .iterate = true });
        var index = try Self.open(std.testing.allocator, dir, 10, true, null);
        defer index.deinit();
        try std.testing.expectEqual(@as(u64, 5000), index.version);
        try std.testing.expectEqual(@as(u64, 41), index.commit_id);
        try std.testing.expectEqual(checkpointed_at, index.file_version);

        // Doc 99 was memory-only, so it comes back through oplog replay. Its position
        // must survive that: replay rebuilds the segment info by hand, and dropping
        // the position there silently reports every replayed doc as position 0.
        var reader = try index.acquireReader();
        defer reader.deinit();
        try std.testing.expectEqual(@as(u64, 5000), reader.getDocInfo(99).?.version);
    }
}

test "versions never go backwards, and an upstream-fed index stays upstream-fed" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_index_version_monotonic";
    cleanupTestDir(cwd, dir_path);
    try cwd.createDir(dir_path, 0o755);
    defer cleanupTestDir(cwd, dir_path);

    {
        const dir = try cwd.openDir(dir_path, .{ .iterate = true });
        var index = try Self.open(std.testing.allocator, dir, 100_000, true, null);
        defer index.deinit();

        _ = try index.update(&[_]Change{.{ .insert = .{ .id = 1, .hashes = &[_]u32{1} } }}, .{ .version = 500 });
        try std.testing.expect(index.segments.value.external_versions);

        // Backwards is refused and writes nothing.
        try std.testing.expectError(error.VersionWentBackwards, index.update(
            &[_]Change{.{ .insert = .{ .id = 2, .hashes = &[_]u32{2} } }},
            .{ .version = 499 },
        ));
        try std.testing.expectEqual(@as(u64, 500), index.version);
        try std.testing.expectEqual(@as(u64, 1), index.commit_id);

        // Equal is allowed: a bootstrap loads a whole snapshot taken at one position,
        // so many commits legitimately share it.
        _ = try index.update(&[_]Change{.{ .insert = .{ .id = 3, .hashes = &[_]u32{3} } }}, .{ .version = 500 });
        try std.testing.expectEqual(@as(u64, 500), index.version);
        try std.testing.expectEqual(@as(u64, 2), index.commit_id);

        // And a positionless write is now refused outright: minting a local version on
        // top of upstream ones would invent a position the upstream never issued.
        try std.testing.expectError(error.VersionRequired, index.update(
            &[_]Change{.{ .insert = .{ .id = 4, .hashes = &[_]u32{4} } }},
            .{},
        ));
    }

    // The marker is sticky across a restart.
    {
        const dir = try cwd.openDir(dir_path, .{ .iterate = true });
        var index = try Self.open(std.testing.allocator, dir, 1, true, null);
        defer index.deinit();
        try std.testing.expectError(error.VersionRequired, index.update(
            &[_]Change{.{ .insert = .{ .id = 5, .hashes = &[_]u32{5} } }},
            .{},
        ));

        // Checkpoint everything, which truncates the WAL that carried the marker...
        _ = try index.update(&[_]Change{.{ .insert = .{ .id = 6, .hashes = &[_]u32{6} } }}, .{ .version = 900 });
        var i: usize = 0;
        while (i < 5) : (i += 1) try index.runMaintenance();
        try std.testing.expect(index.segments.value.file.len > 0);
    }

    // ...and it still survives, because the manifest carries it too.
    {
        const dir = try cwd.openDir(dir_path, .{ .iterate = true });
        var index = try Self.open(std.testing.allocator, dir, 1, true, null);
        defer index.deinit();
        try std.testing.expectError(error.VersionRequired, index.update(
            &[_]Change{.{ .insert = .{ .id = 7, .hashes = &[_]u32{7} } }},
            .{},
        ));
    }
}

test "a standalone index mints its own versions and is not poisoned" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_index_standalone_versions";
    cleanupTestDir(cwd, dir_path);
    try cwd.createDir(dir_path, 0o755);
    defer cleanupTestDir(cwd, dir_path);

    const dir = try cwd.openDir(dir_path, .{ .iterate = true });
    var index = try Self.open(std.testing.allocator, dir, 100_000, true, null);
    defer index.deinit();

    // With no upstream, version and commit id coincide exactly as before the split.
    var id: u32 = 1;
    while (id <= 3) : (id += 1) {
        const v = try index.update(&[_]Change{.{ .insert = .{ .id = id, .hashes = &[_]u32{id} } }}, .{});
        try std.testing.expectEqual(@as(u64, id), v);
    }
    try std.testing.expectEqual(index.commit_id, index.version);
}

test "a restored index continues commit ids instead of restarting them" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_index_restored_commit_ids";
    cleanupTestDir(cwd, dir_path);
    try cwd.createDir(dir_path, 0o755);
    defer cleanupTestDir(cwd, dir_path);

    var restored_commit_id: u64 = 0;
    {
        const dir = try cwd.openDir(dir_path, .{ .iterate = true });
        var index = try Self.open(std.testing.allocator, dir, 1, true, null);
        defer index.deinit();
        var id: u32 = 1;
        while (id <= 20) : (id += 1) {
            _ = try index.update(&[_]Change{.{ .insert = .{ .id = id, .hashes = &[_]u32{id} } }}, .{});
            try index.runMaintenance();
        }
        try std.testing.expect(index.segments.value.file.len > 0);
        restored_commit_id = index.commit_id;
    }

    // A peer bootstrap swaps in restored segments and deletes the WAL outright
    // (MultiIndex.swapAndReopen), so the oplog comes back empty while the segments
    // carry commit ids from the donor.
    {
        var dir = try cwd.openDir(dir_path, .{ .iterate = true });
        defer dir.close();
        @import("common.zig").deleteDirTree(std.testing.allocator, dir, "oplog") catch {};
    }

    {
        const dir = try cwd.openDir(dir_path, .{ .iterate = true });
        var index = try Self.open(std.testing.allocator, dir, 100_000, true, null);
        defer index.deinit();
        try std.testing.expectEqual(restored_commit_id, index.commit_id);

        // The next commit must continue the sequence. Restarting at 1 would collide
        // with the restored segments' ids and break their dense tiling.
        _ = try index.update(&[_]Change{.{ .insert = .{ .id = 99, .hashes = &[_]u32{99} } }}, .{});
        try std.testing.expectEqual(restored_commit_id + 1, index.commit_id);

        // And merging still holds, which is what the tiling exists for.
        var i: usize = 0;
        while (i < 5) : (i += 1) try index.runMaintenance();
    }
}
