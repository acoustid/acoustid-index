const std = @import("std");
const log = std.log.scoped(.index);

const zul = @import("zul");

const Deadline = @import("utils/Deadline.zig");
const Change = @import("change.zig").Change;
const SearchResult = @import("common.zig").SearchResult;
const SearchResults = @import("common.zig").SearchResults;
const SegmentID = @import("common.zig").SegmentID;

const Oplog = @import("Oplog.zig");

const MemorySegment = @import("MemorySegment.zig");
const MemorySegmentList = MemorySegment.List;
const MemorySegmentNode = MemorySegment.List.List.Node;

const FileSegment = @import("FileSegment.zig");
const FileSegmentList = FileSegment.List;
const FileSegmentNode = FileSegment.List.List.Node;

const SegmentMerger = @import("segment_merger.zig").SegmentMerger;

const filefmt = @import("filefmt.zig");

const Self = @This();

const Options = struct {
    create: bool = false,
    min_segment_size: usize = 1_000_000,
    max_segment_size: usize = 100_000_000,
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
segments_lock: std.Thread.RwLock = .{},

// Mutex used to control exclusivity during re-shuffling of segment lists during checkpoint/merges.
// This lock by itself doesn't give access to either list, you need to hold the segments_lock as well.
file_segments_lock: std.Thread.Mutex = .{},

// Mutex used to control linearity of updates.
update_lock: std.Thread.Mutex = .{},

// Mutex used to control merging of in-memory segments.
memory_merge_lock: std.Thread.Mutex = .{},

file_segments_to_delete: std.ArrayList(SegmentID),

checkpoint_mutex: std.Thread.Mutex = .{},
checkpoint_condition: std.Thread.Condition = .{},
checkpoint_stop: bool = false,
checkpoint_thread: ?std.Thread = null,

file_segment_merge_mutex: std.Thread.Mutex = .{},
file_segment_merge_condition: std.Thread.Condition = .{},
file_segment_merge_stop: bool = false,
file_segment_merge_thread: ?std.Thread = null,

pub fn init(allocator: std.mem.Allocator, dir: std.fs.Dir, options: Options) !Self {
    var data_dir = try dir.makeOpenPath("data", .{ .iterate = true });
    errdefer data_dir.close();

    var oplog_dir = try dir.makeOpenPath("oplog", .{ .iterate = true });
    errdefer oplog_dir.close();

    return .{
        .options = options,
        .allocator = allocator,
        .data_dir = data_dir,
        .oplog_dir = oplog_dir,
        .oplog = Oplog.init(allocator, oplog_dir),
        .file_segments = FileSegmentList.init(allocator),
        .file_segments_to_delete = std.ArrayList(SegmentID).init(allocator),
        .memory_segments = MemorySegmentList.init(allocator),
    };
}

pub fn deinit(self: *Self) void {
    self.stopCheckpointThread();
    self.stopFileSegmentMergeThread();

    self.oplog.deinit();
    self.memory_segments.deinit();
    self.file_segments.deinit();
    self.oplog_dir.close();
    self.data_dir.close();
}

fn flattenMemorySegmentIds(self: *Self) void {
    var iter = self.memory_segments.segments.first;
    while (iter) |node| : (iter = node.next) {
        if (!node.data.frozen) {
            if (node.prev) |prev| {
                node.data.id = prev.data.id.next();
            } else {
                node.data.id.included_merges = 0;
            }
        }
    }
}

fn prepareMemorySegmentMerge(self: *Self) !?MemorySegmentList.PreparedMerge {
    self.segments_lock.lockShared();
    defer self.segments_lock.unlockShared();

    const merge = try self.memory_segments.prepareMerge(.{ .max_segment_size = self.options.min_segment_size }) orelse return null;
    errdefer self.memory_segments.destroySegment(merge.target);

    try merge.target.data.merge(&merge.sources.node1.data, &merge.sources.node2.data, &self.memory_segments);

    return merge;
}

fn finishMemorySegmentMerge(self: *Self, merge: MemorySegmentList.PreparedMerge) bool {
    defer self.memory_segments.destroySegment(merge.sources.node1);
    defer self.memory_segments.destroySegment(merge.sources.node2);

    self.segments_lock.lock();
    defer self.segments_lock.unlock();

    self.memory_segments.applyMerge(merge);

    self.flattenMemorySegmentIds();

    return merge.target.data.getSize() >= self.options.min_segment_size;
}

// Perform partial compaction on the in-memory segments.
fn maybeMergeMemorySegments(self: *Self) !bool {
    self.memory_merge_lock.lock();
    defer self.memory_merge_lock.unlock();

    const merge = try self.prepareMemorySegmentMerge() orelse return false;
    return self.finishMemorySegmentMerge(merge);
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
        if (err == error.FileNotFound and self.options.create) {
            try filefmt.writeIndexFile(self.data_dir, &[_]SegmentID{});
            return;
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

    var src = try self.readyForCheckpoint() orelse return false;

    var src_reader = src.data.reader();
    defer src_reader.close();

    var dest = try self.file_segments.createSegment();
    errdefer self.file_segments.destroySegment(dest);

    try dest.data.build(self.data_dir, &src_reader);

    errdefer dest.data.delete(self.data_dir);

    self.file_segments_lock.lock();
    defer self.file_segments_lock.unlock();

    var ids = try self.getFileSegmentIds();
    defer ids.deinit();

    try ids.append(dest.data.id);

    try filefmt.writeIndexFile(self.data_dir, ids.items);

    self.segments_lock.lock();
    defer self.segments_lock.unlock();

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
    const min_delay = std.time.ns_per_s;
    const max_delay = std.time.ns_per_s * 60;
    var delay: u64 = min_delay;
    var retries: u32 = 0;

    while (true) {
        self.checkpoint_mutex.lock();
        defer self.checkpoint_mutex.unlock();

        if (self.checkpoint_stop) return;

        var wait: bool = true;

        if (self.doCheckpoint()) |successful| {
            delay = min_delay;
            retries = 0;
            if (successful) {
                wait = false;
                self.file_segment_merge_condition.signal();
            }
        } else |err| {
            delay = @min(delay * 110 / 100, max_delay);
            retries += 1;
            log.err("checkpoint failed: {} (retry {})", .{ err, retries });
        }

        if (wait) {
            self.checkpoint_condition.timedWait(&self.checkpoint_mutex, delay) catch {};
        }
    }
}

fn startCheckpointThread(self: *Self) !void {
    if (self.checkpoint_thread != null) return;

    self.checkpoint_mutex.lock();
    self.checkpoint_stop = false;
    self.checkpoint_mutex.unlock();

    self.checkpoint_thread = try std.Thread.spawn(.{}, checkpointThreadFn, .{self});
}

fn stopCheckpointThread(self: *Self) void {
    self.checkpoint_mutex.lock();
    self.checkpoint_stop = true;
    self.checkpoint_condition.broadcast();
    self.checkpoint_mutex.unlock();

    if (self.checkpoint_thread) |thread| {
        thread.join();
    }

    self.checkpoint_thread = null;
}

fn fileSegmentMergeThreadFn(self: *Self) void {
    const min_delay = std.time.ns_per_s;
    const max_delay = std.time.ns_per_s * 60;
    var delay: u64 = min_delay;
    var retries: u32 = 0;

    while (true) {
        self.file_segment_merge_mutex.lock();
        defer self.file_segment_merge_mutex.unlock();

        if (self.file_segment_merge_stop) return;

        var wait: bool = true;

        if (self.maybeMergeFileSegments()) |successful| {
            delay = min_delay;
            retries = 0;
            if (successful) {
                wait = false;
            }
        } else |err| {
            delay = @min(delay * 110 / 100, max_delay);
            retries += 1;
            log.err("file segment merge failed: {} (retry {})", .{ err, retries });
        }

        if (wait) {
            self.checkpoint_condition.timedWait(&self.file_segment_merge_mutex, delay) catch {};
        }
    }
}

fn startFileSegmentMergeThread(self: *Self) !void {
    if (self.file_segment_merge_thread != null) return;

    self.file_segment_merge_mutex.lock();
    self.file_segment_merge_stop = false;
    self.file_segment_merge_mutex.unlock();

    self.file_segment_merge_thread = try std.Thread.spawn(.{}, fileSegmentMergeThreadFn, .{self});
}

fn stopFileSegmentMergeThread(self: *Self) void {
    self.file_segment_merge_mutex.lock();
    self.file_segment_merge_stop = true;
    self.file_segment_merge_condition.broadcast();
    self.file_segment_merge_mutex.unlock();

    if (self.file_segment_merge_thread) |thread| {
        thread.join();
    }

    self.file_segment_merge_thread = null;
}

fn prepareFileSegmentMerge(self: *Self) !?FileSegmentList.PreparedMerge {
    self.segments_lock.lockShared();
    defer self.segments_lock.unlockShared();

    const merge = try self.file_segments.prepareMerge(.{ .max_segment_size = self.options.max_segment_size }) orelse return null;
    errdefer self.file_segments.destroySegment(merge.target);

    var merger = SegmentMerger(FileSegment).init(self.allocator, &self.file_segments);
    defer merger.deinit();

    try merger.addSource(&merge.sources.node1.data);
    try merger.addSource(&merge.sources.node2.data);
    try merger.prepare();

    try merge.target.data.build(self.data_dir, &merger);

    return merge;
}

fn finishFileSegmentMerge(self: *Self, merge: FileSegmentList.PreparedMerge) !void {
    self.file_segments_lock.lock();
    defer self.file_segments_lock.unlock();

    try self.file_segments_to_delete.ensureUnusedCapacity(3);

    errdefer self.file_segments.destroySegment(merge.target);
    errdefer merge.target.data.delete(self.data_dir);

    var ids = try self.getFileSegmentIds();
    defer ids.deinit();

    var index1: usize = 0;
    var index2: usize = 0;

    var i: usize = 0;
    while (i < ids.items.len) : (i += 1) {
        if (SegmentID.eq(ids.items[i], merge.sources.node1.data.id)) {
            index1 = i;
        }
        if (SegmentID.eq(ids.items[i], merge.sources.node2.data.id)) {
            index2 = i;
        }
    }

    std.debug.assert(index1 + 1 == index2);

    try ids.replaceRange(index1, 2, &[_]SegmentID{merge.target.data.id});

    std.debug.assert(std.sort.isSorted(SegmentID, ids.items, {}, SegmentID.cmp));

    try filefmt.writeIndexFile(self.data_dir, ids.items);

    defer self.file_segments.destroySegment(merge.sources.node1);
    defer self.file_segments.destroySegment(merge.sources.node2);

    defer merge.sources.node1.data.delete(self.data_dir);
    defer merge.sources.node2.data.delete(self.data_dir);

    self.segments_lock.lock();
    defer self.segments_lock.unlock();

    self.file_segments.applyMerge(merge);

    log.info("committed merge segment {}:{}", .{ merge.target.data.id.version, merge.target.data.id.included_merges });
}

pub fn maybeMergeFileSegments(self: *Self) !bool {
    const merge = try self.prepareFileSegmentMerge() orelse return false;
    try self.finishFileSegmentMerge(merge);
    return true;
}

pub fn open(self: *Self) !void {
    try self.startCheckpointThread();
    try self.startFileSegmentMergeThread();
    try self.loadSegments();
    try self.oplog.open(self.getMaxCommitId(), Updater{ .index = self });
}

const Checkpoint = struct {
    src: *MemorySegmentNode,
    dest: ?*FileSegmentNode = null,
};

fn readyForCheckpoint(self: *Self) !?*MemorySegmentNode {
    self.segments_lock.lockShared();
    defer self.segments_lock.unlockShared();

    if (self.memory_segments.segments.first) |first_node| {
        if (first_node.data.getSize() > self.options.min_segment_size) {
            return first_node;
        }
    }

    return null;
}

fn getFileSegmentIds(self: *Self) !std.ArrayList(SegmentID) {
    var segment_ids = std.ArrayList(SegmentID).init(self.allocator);
    errdefer segment_ids.deinit();

    self.segments_lock.lockShared();
    defer self.segments_lock.unlockShared();

    try self.file_segments.getIds(&segment_ids);

    return segment_ids;
}

pub fn update(self: *Self, changes: []const Change) !void {
    const start_checkpoint = try self.maybeMergeMemorySegments();
    if (start_checkpoint) {
        self.checkpoint_condition.signal();
    }
    try self.oplog.write(changes, Updater{ .index = self });
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
