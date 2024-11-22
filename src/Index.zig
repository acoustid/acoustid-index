const std = @import("std");
const log = std.log.scoped(.index);

const zul = @import("zul");

const Deadline = @import("utils/Deadline.zig");
const Change = @import("change.zig").Change;
const SearchResult = @import("common.zig").SearchResult;
const SearchResults = @import("common.zig").SearchResults;
const SegmentID = @import("common.zig").SegmentID;

const SegmentMergeOptions = @import("segment_list.zig").SegmentMergeOptions;
const SegmentList = @import("segment_list.zig").SegmentList;

const Oplog = @import("Oplog.zig");

const MemorySegment = @import("MemorySegment.zig");
const MemorySegmentList = SegmentList(MemorySegment);
const MemorySegmentNode = MemorySegmentList.Node;

const FileSegment = @import("FileSegment.zig");
const FileSegmentList = SegmentList(FileSegment);
const FileSegmentNode = FileSegmentList.Node;

const SegmentMerger = @import("segment_merger.zig").SegmentMerger;

const TieredMergePolicy = @import("segment_merge_policy.zig").TieredMergePolicy;

const filefmt = @import("filefmt.zig");

const Self = @This();

const Options = struct {
    create: bool = false,
    min_segment_size: usize = 250_000,
    max_segment_size: usize = 1_000_000_000,
};

options: Options,
allocator: std.mem.Allocator,

data_dir: std.fs.Dir,
oplog_dir: std.fs.Dir,

oplog: Oplog,

file_segments: FileSegmentList,
memory_segments: MemorySegmentList,

// RW lock used to control general mutations to either file_segments or memory_segments.
// This lock needs to be held for any read/write operations on either list.
// Once you hold this lock, you can be sure that no changes are happening to either list.
segments_lock: std.Thread.RwLock = .{},

// These locks give partial access to the respective segments list.
//   1) For memory_segments, new segment can be appended to the list without this lock.
//   2) For file_segments, no write operation can happen without this lock.
// These lock can be only acquired before segments_lock, never after, to avoid deadlock situatons.
// They are mostly useful to allowing read access to segments during merge/checkpoint, without blocking real-time update.
file_segments_lock: std.Thread.Mutex = .{},
memory_segments_lock: std.Thread.Mutex = .{},

// Mutex used to control linearity of updates.
update_lock: std.Thread.Mutex = .{},

stopping: std.atomic.Value(bool),

checkpoint_event: std.Thread.ResetEvent = .{},
checkpoint_thread: ?std.Thread = null,

file_segment_merge_event: std.Thread.ResetEvent = .{},
file_segment_merge_thread: ?std.Thread = null,

memory_segment_merge_event: std.Thread.ResetEvent = .{},
memory_segment_merge_thread: ?std.Thread = null,

pub fn init(allocator: std.mem.Allocator, dir: std.fs.Dir, options: Options) !Self {
    var data_dir = try dir.makeOpenPath("data", .{ .iterate = true });
    errdefer data_dir.close();

    var oplog_dir = try dir.makeOpenPath("oplog", .{ .iterate = true });
    errdefer oplog_dir.close();

    const file_segment_merge_policy = TieredMergePolicy(FileSegment){
        .min_segment_size = options.min_segment_size,
        .max_segment_size = options.max_segment_size,
        .segments_per_level = 10,
        .segments_per_merge = 10,
    };

    const memory_segment_merge_policy = TieredMergePolicy(MemorySegment){
        .min_segment_size = 100,
        .max_segment_size = options.min_segment_size,
        .segments_per_level = 5,
        .segments_per_merge = 10,
        .max_segments = 16,
    };

    return .{
        .options = options,
        .allocator = allocator,
        .data_dir = data_dir,
        .oplog_dir = oplog_dir,
        .oplog = Oplog.init(allocator, oplog_dir),
        .file_segments = FileSegmentList.init(allocator, file_segment_merge_policy),
        .memory_segments = MemorySegmentList.init(allocator, memory_segment_merge_policy),
        .stopping = std.atomic.Value(bool).init(false),
    };
}

pub fn deinit(self: *Self) void {
    self.stopping.store(true, .release);

    self.stopCheckpointThread();
    self.stopFileSegmentMergeThread();
    self.stopMemorySegmentMergeThread();

    self.oplog.deinit();
    self.memory_segments.deinit();
    self.file_segments.deinit();
    self.oplog_dir.close();
    self.data_dir.close();
}

fn flattenMemorySegmentIds(self: *Self) void {
    var iter = self.memory_segments.segments.first;
    var prev_node: @TypeOf(iter) = null;
    while (iter) |node| : (iter = node.next) {
        if (!node.data.frozen) {
            if (prev_node) |prev| {
                node.data.id = prev.data.id.next();
            } else {
                node.data.id.included_merges = 0;
            }
        }
        prev_node = node;
    }
}

fn prepareMemorySegmentMerge(self: *Self) !?MemorySegmentList.PreparedMerge {
    self.segments_lock.lockShared();
    defer self.segments_lock.unlockShared();

    return try self.memory_segments.prepareMerge() orelse return null;
}

fn maybeMergeMemorySegments(self: *Self) !bool {
    var merge = try self.prepareMemorySegmentMerge() orelse return false;
    defer merge.merger.deinit();
    errdefer self.memory_segments.destroySegment(merge.target);

    // here we are accessing the segment without any lock, but it's OK, because we are the only thread
    // that can delete a segment
    try merge.target.data.merge(&merge.merger);

    self.memory_segments_lock.lock();
    defer self.memory_segments_lock.unlock();

    defer self.memory_segments.cleanupAfterMerge(merge, .{});

    self.segments_lock.lock();
    defer self.segments_lock.unlock();

    self.memory_segments.applyMerge(merge);

    self.flattenMemorySegmentIds();

    if (merge.target.data.getSize() > self.options.min_segment_size / 2) {
        log.info("performed big memory merge, size={}", .{merge.target.data.getSize()});
    }

    return merge.target.data.getSize() >= self.options.min_segment_size;
}

pub const PendingUpdate = struct {
    node: *MemorySegmentNode,
    finished: bool = false,
};

// Prepares update for later commit, will block until previous update has been committed.
fn prepareUpdate(self: *Self, changes: []const Change) !PendingUpdate {
    const node = try self.memory_segments.createSegment();
    errdefer self.memory_segments.destroySegment(node);

    try node.data.build(changes);

    self.update_lock.lock();
    return .{ .node = node };
}

// Commits the update, does nothing if it has already been cancelled or committted.
fn commitUpdate(self: *Self, pending_update: *PendingUpdate, commit_id: u64) void {
    if (pending_update.finished) return;

    self.segments_lock.lock();
    defer self.segments_lock.unlock();

    self.memory_segments.appendSegment(pending_update.node);

    pending_update.node.data.max_commit_id = commit_id;
    if (pending_update.node.prev) |prev| {
        pending_update.node.data.id = prev.data.id.next();
    } else {
        if (self.file_segments.segments.last) |last_file_segment| {
            pending_update.node.data.id = last_file_segment.data.id.next();
        } else {
            pending_update.node.data.id = SegmentID.first();
        }
    }

    pending_update.finished = true;
    self.update_lock.unlock();
}

// Cancels the update, does nothing if it has already been cancelled or committted.
fn cancelUpdate(self: *Self, pending_update: *PendingUpdate) void {
    if (pending_update.finished) return;

    self.memory_segments.destroySegment(pending_update.node);

    pending_update.finished = true;
    self.update_lock.unlock();
}

const Updater = struct {
    index: *Self,

    pub fn prepareUpdate(self: Updater, changes: []const Change) !PendingUpdate {
        return self.index.prepareUpdate(changes);
    }

    pub fn commitUpdate(self: Updater, pending_update: *PendingUpdate, commit_id: u64) void {
        self.index.commitUpdate(pending_update, commit_id);
    }

    pub fn cancelUpdate(self: Updater, pending_update: *PendingUpdate) void {
        self.index.cancelUpdate(pending_update);
    }
};

fn loadSegment(self: *Self, segment_id: SegmentID) !void {
    const node = try self.file_segments.createSegment();
    errdefer self.file_segments.destroySegment(node);

    try node.data.open(self.data_dir, segment_id);

    self.segments_lock.lock();
    defer self.segments_lock.unlock();

    self.file_segments.appendSegment(node);
}

fn loadSegments(self: *Self) !void {
    const segment_ids = filefmt.readIndexFile(self.data_dir, self.allocator) catch |err| {
        if (err == error.FileNotFound) {
            if (self.options.create) {
                try filefmt.writeIndexFile(self.data_dir, &[_]SegmentID{});
                return;
            }
            return error.IndexNotFound;
        }
        return err;
    };
    defer self.allocator.free(segment_ids);

    for (segment_ids) |segment_id| {
        try self.loadSegment(segment_id);
    }
}

fn doCheckpoint(self: *Self) !bool {
    const start_time = std.time.milliTimestamp();

    var src = self.readyForCheckpoint() orelse return false;

    var src_reader = src.data.reader();
    defer src_reader.close();

    var dest = try self.file_segments.createSegment();
    errdefer self.file_segments.destroySegment(dest);

    try dest.data.build(self.data_dir, &src_reader);

    errdefer dest.data.delete(self.data_dir);

    self.file_segments_lock.lock();
    defer self.file_segments_lock.unlock();

    var ids = try self.file_segments.getIdsAfterAppend(dest, self.allocator);
    defer ids.deinit();

    try filefmt.writeIndexFile(self.data_dir, ids.items);

    // we are about to remove segment from the memory_segments list
    self.memory_segments_lock.lock();
    defer self.memory_segments_lock.unlock();

    self.segments_lock.lock();
    defer self.segments_lock.unlock();

    log.info("stage stats size={}, len={}", .{ self.memory_segments.getTotalSize(), self.memory_segments.segments.len });

    if (src != self.memory_segments.segments.first) {
        std.debug.panic("checkpoint node is not first in list", .{});
    }

    if (self.file_segments.segments.last) |last_file_segment| {
        if (last_file_segment.data.id.version >= dest.data.id.version) {
            std.debug.panic("inconsistent versions between memory and file segments", .{});
        }
    }

    self.file_segments.appendSegment(dest);
    self.memory_segments.removeAndDestroySegment(src);

    log.info("saved changes up to commit {} to disk", .{dest.data.max_commit_id});

    const end_time = std.time.milliTimestamp();
    log.info("checkpoint took {} ms", .{end_time - start_time});
    return true;
}

fn checkpointThreadFn(self: *Self) void {
    while (!self.stopping.load(.acquire)) {
        if (self.doCheckpoint()) |successful| {
            if (successful) {
                self.scheduleFileSegmentMerge();
                continue;
            }
            self.checkpoint_event.reset();
        } else |err| {
            log.err("checkpoint failed: {}", .{err});
        }
        self.checkpoint_event.timedWait(std.time.ns_per_min) catch continue;
    }
}

fn startCheckpointThread(self: *Self) !void {
    if (self.checkpoint_thread != null) return;

    log.info("starting checkpoint thread", .{});
    self.checkpoint_thread = try std.Thread.spawn(.{}, checkpointThreadFn, .{self});
}

fn stopCheckpointThread(self: *Self) void {
    if (self.checkpoint_thread) |thread| {
        self.checkpoint_event.set();
        thread.join();
    }
    self.checkpoint_thread = null;
}

fn fileSegmentMergeThreadFn(self: *Self) void {
    while (!self.stopping.load(.acquire)) {
        if (self.maybeMergeFileSegments()) |successful| {
            if (successful) {
                continue;
            }
            self.file_segment_merge_event.reset();
        } else |err| {
            log.err("file segment merge failed: {}", .{err});
        }
        self.file_segment_merge_event.timedWait(std.time.ns_per_min) catch continue;
    }
}

fn startFileSegmentMergeThread(self: *Self) !void {
    if (self.file_segment_merge_thread != null) return;

    log.info("starting file segment merge thread", .{});
    self.file_segment_merge_thread = try std.Thread.spawn(.{}, fileSegmentMergeThreadFn, .{self});
}

fn stopFileSegmentMergeThread(self: *Self) void {
    if (self.file_segment_merge_thread) |thread| {
        self.file_segment_merge_event.set();
        thread.join();
    }
    self.file_segment_merge_thread = null;
}

fn prepareFileSegmentMerge(self: *Self) !?FileSegmentList.PreparedMerge {
    self.segments_lock.lockShared();
    defer self.segments_lock.unlockShared();

    return try self.file_segments.prepareMerge();
}

fn maybeMergeFileSegments(self: *Self) !bool {
    var merge = try self.prepareFileSegmentMerge() orelse return false;
    defer merge.merger.deinit();
    errdefer self.file_segments.destroySegment(merge.target);

    // We are reading segment data without holding any lock here,
    // but it's OK, because are the only ones modifying segments.
    // The only other place with write access to the segment list is
    // the checkpoint thread, which is only ever adding new segments.
    try merge.target.data.build(self.data_dir, &merge.merger);
    errdefer merge.target.data.delete(self.data_dir);

    // By acquiring file_segments_lock, we make sure that the file_segments list
    // can't be modified by other threads.
    self.file_segments_lock.lock();
    defer self.file_segments_lock.unlock();

    var ids = try self.file_segments.getIdsAfterAppliedMerge(merge, self.allocator);
    defer ids.deinit();

    try filefmt.writeIndexFile(self.data_dir, ids.items);

    // We want to do this outside of segments_lock to avoid blocking searches more than necessary
    defer self.file_segments.cleanupAfterMerge(merge, .{self.data_dir});

    // This lock allows to modify the file_segments list, it's blocking all other threads.
    self.segments_lock.lock();
    defer self.segments_lock.unlock();

    self.file_segments.applyMerge(merge);

    log.info("committed merge segment {}:{}", .{ merge.target.data.id.version, merge.target.data.id.included_merges });
    return true;
}

fn memorySegmentMergeThreadFn(self: *Self) void {
    while (!self.stopping.load(.acquire)) {
        if (self.maybeMergeMemorySegments()) |successful| {
            if (successful) {
                self.checkpoint_event.set();
                continue;
            }
            self.memory_segment_merge_event.reset();
        } else |err| {
            log.err("memory segment merge failed: {}", .{err});
        }
        self.memory_segment_merge_event.timedWait(std.time.ns_per_min) catch continue;
    }
}

fn startMemorySegmentMergeThread(self: *Self) !void {
    if (self.memory_segment_merge_thread != null) return;

    log.info("starting memory segment merge thread", .{});
    self.memory_segment_merge_thread = try std.Thread.spawn(.{}, memorySegmentMergeThreadFn, .{self});
}

fn stopMemorySegmentMergeThread(self: *Self) void {
    if (self.memory_segment_merge_thread) |thread| {
        self.memory_segment_merge_event.set();
        thread.join();
    }
    self.memory_segment_merge_thread = null;
}

pub fn open(self: *Self) !void {
    try self.loadSegments();
    try self.oplog.open(self.getMaxCommitId(), Updater{ .index = self });
    try self.startCheckpointThread();
    try self.startFileSegmentMergeThread();
    try self.startMemorySegmentMergeThread();
}

const Checkpoint = struct {
    src: *MemorySegmentNode,
    dest: ?*FileSegmentNode = null,
};

fn readyForCheckpoint(self: *Self) ?*MemorySegmentNode {
    self.segments_lock.lockShared();
    defer self.segments_lock.unlockShared();

    if (self.memory_segments.segments.first) |first_node| {
        if (first_node.data.getSize() > self.options.min_segment_size) {
            return first_node;
        }
    }

    return null;
}

fn scheduleCheckpoint(self: *Self) void {
    self.checkpoint_event.set();
}

fn scheduleMemorySegmentMerge(self: *Self) void {
    self.segments_lock.lockShared();
    defer self.segments_lock.unlockShared();

    if (self.memory_segments.needsMerge()) {
        self.memory_segment_merge_event.set();
    }
}

fn scheduleFileSegmentMerge(self: *Self) void {
    self.segments_lock.lockShared();
    defer self.segments_lock.unlockShared();

    if (self.file_segments.needsMerge()) {
        self.file_segment_merge_event.set();
    }
}

pub fn update(self: *Self, changes: []const Change) !void {
    try self.oplog.write(changes, Updater{ .index = self });
    self.scheduleMemorySegmentMerge();
}

pub fn search(self: *Self, hashes: []const u32, allocator: std.mem.Allocator, deadline: Deadline) !SearchResults {
    const sorted_hashes = try allocator.dupe(u32, hashes);
    defer allocator.free(sorted_hashes);
    std.sort.pdq(u32, sorted_hashes, {}, std.sort.asc(u32));

    var results = SearchResults.init(allocator);
    errdefer results.deinit();

    self.segments_lock.lockShared();
    defer self.segments_lock.unlockShared();

    try self.file_segments.search(sorted_hashes, &results, deadline);
    try self.memory_segments.search(sorted_hashes, &results, deadline);

    results.sort();
    return results;
}

pub fn getMaxCommitId(self: *Self) u64 {
    self.segments_lock.lockShared();
    defer self.segments_lock.unlockShared();

    return @max(self.file_segments.getMaxCommitId(), self.memory_segments.getMaxCommitId());
}

test {
    _ = @import("index_tests.zig");
}
