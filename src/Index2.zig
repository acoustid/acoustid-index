const std = @import("std");
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.index);

const zul = @import("zul");

const Deadline = @import("utils/Deadline.zig");
const Change = @import("change.zig").Change;
const SearchResult = @import("common.zig").SearchResult;
const SearchResults = @import("common.zig").SearchResults;
const SegmentId = @import("common.zig").SegmentId;

const Oplog = @import("Oplog.zig");

const SegmentList = @import("segment_list2.zig").SegmentList;
const SegmentListManager = @import("segment_list2.zig").SegmentListManager;

const MemorySegment = @import("MemorySegment.zig");
const MemorySegmentList = SegmentList(MemorySegment);
const MemorySegmentNode = MemorySegmentList.Node;

const FileSegment = @import("FileSegment.zig");
const FileSegmentList = SegmentList(FileSegment);
const FileSegmentNode = FileSegmentList.Node;

const SharedPtr = @import("utils/smartptr.zig").SharedPtr;

const SegmentMerger = @import("segment_merger.zig").SegmentMerger;

const TieredMergePolicy = @import("segment_merge_policy.zig").TieredMergePolicy;

const filefmt = @import("filefmt.zig");

const Self = @This();

const Options = struct {
    create: bool = false,
    min_segment_size: usize = 250_000,
    max_segment_size: usize = 500_000_000,
};

options: Options,
allocator: std.mem.Allocator,

data_dir: std.fs.Dir,
oplog_dir: std.fs.Dir,

oplog: Oplog,

memory_segments: SegmentListManager(MemorySegment),
file_segments: SegmentListManager(FileSegment),

// These segments are owned by the index and can't be accessed without acquiring segments_lock.
// They can never be modified, only replaced.
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

fn getFileSegmentSize(segment: SharedPtr(FileSegment)) usize {
    return segment.value.getSize();
}

fn getMemorySegmentSize(segment: SharedPtr(MemorySegment)) usize {
    return segment.value.getSize();
}

pub fn init(allocator: std.mem.Allocator, dir: std.fs.Dir, options: Options) !Self {
    var data_dir = try dir.makeOpenPath("data", .{ .iterate = true });
    errdefer data_dir.close();

    var oplog_dir = try dir.makeOpenPath("oplog", .{ .iterate = true });
    errdefer oplog_dir.close();

    const memory_segments = try SegmentListManager(MemorySegment).init(
        allocator,
        .{},
        .{
            .min_segment_size = 100,
            .max_segment_size = options.min_segment_size,
            .segments_per_level = 5,
            .segments_per_merge = 10,
            .max_segments = 16,
        },
    );

    const file_segments = try SegmentListManager(FileSegment).init(
        allocator,
        .{
            .dir = data_dir,
        },
        .{
            .min_segment_size = options.min_segment_size,
            .max_segment_size = options.max_segment_size,
            .segments_per_level = 10,
            .segments_per_merge = 10,
        },
    );

    return .{
        .options = options,
        .allocator = allocator,
        .data_dir = data_dir,
        .oplog_dir = oplog_dir,
        .oplog = Oplog.init(allocator, oplog_dir),
        .segments_lock = .{},
        .memory_segments = memory_segments,
        .file_segments = file_segments,
        .stopping = std.atomic.Value(bool).init(false),
    };
}

pub fn deinit(self: *Self) void {
    self.stopping.store(true, .release);

    self.stopCheckpointThread();
    self.stopMemorySegmentMergeThread();
    self.stopFileSegmentMergeThread();

    self.memory_segments.deinit();
    self.file_segments.deinit();

    self.oplog.deinit();
    self.oplog_dir.close();
    self.data_dir.close();
}

pub const PendingUpdate = struct {
    node: MemorySegmentNode,
    segments: SharedPtr(MemorySegmentList),
    finished: bool = false,

    pub fn deinit(self: *@This(), allocator: Allocator) void {
        if (self.finished) return;
        self.segments.release(allocator, .{allocator});
        MemorySegmentList.destroySegment(allocator, &self.node);
        self.finished = true;
    }
};

// Prepares update for later commit, will block until previous update has been committed.
fn prepareUpdate(self: *Self, changes: []const Change) !PendingUpdate {
    var node = try MemorySegmentList.createSegment(self.allocator, .{});
    errdefer MemorySegmentList.destroySegment(self.allocator, &node);

    try node.value.build(changes);

    self.update_lock.lock();
    errdefer self.update_lock.unlock();

    self.segments_lock.lockShared();
    defer self.segments_lock.unlockShared();

    const segments = try MemorySegmentList.createShared(self.allocator, self.memory_segments.count() + 1);

    return .{ .node = node, .segments = segments };
}

// Commits the update, does nothing if it has already been cancelled or committted.
fn commitUpdate(self: *Self, pending_update: *PendingUpdate, commit_id: u64) void {
    if (pending_update.finished) return;

    defer pending_update.deinit(self.allocator);

    self.segments_lock.lock();
    defer self.segments_lock.unlock();

    pending_update.node.value.max_commit_id = commit_id;
    pending_update.node.value.id = blk: {
        if (self.memory_segments.segments.value.getLast()) |n| {
            break :blk n.value.id.next();
        } else if (self.file_segments.segments.value.getFirst()) |n| {
            break :blk n.value.id.next();
        } else {
            break :blk SegmentId.first();
        }
    };

    self.memory_segments.segments.value.appendSegmentInto(pending_update.segments.value, pending_update.node);

    self.memory_segments.segments.swap(&pending_update.segments);

    pending_update.finished = true;
    self.update_lock.unlock();
}

// Cancels the update, does nothing if it has already been cancelled or committted.
fn cancelUpdate(self: *Self, pending_update: *PendingUpdate) void {
    if (pending_update.finished) return;

    defer pending_update.deinit(self.allocator);

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

fn loadSegment(self: *Self, segment_id: SegmentId) !FileSegmentNode {
    var node = try FileSegmentList.createSegment(self.allocator, .{ .dir = self.data_dir });
    errdefer FileSegmentList.destroySegment(self.allocator, &node);

    try node.value.open(segment_id);

    return node;
}

fn loadSegments(self: *Self) !void {
    self.segments_lock.lock();
    defer self.segments_lock.unlock();

    const segment_ids = filefmt.readIndexFile(self.data_dir, self.allocator) catch |err| {
        if (err == error.FileNotFound) {
            if (self.options.create) {
                try self.updateIndexFile(self.file_segments.segments.value);
                return;
            }
            return error.IndexNotFound;
        }
        return err;
    };
    defer self.allocator.free(segment_ids);

    try self.file_segments.segments.value.nodes.ensureTotalCapacity(self.allocator, segment_ids.len);

    for (segment_ids) |segment_id| {
        const node = try self.loadSegment(segment_id);
        self.file_segments.segments.value.nodes.appendAssumeCapacity(node);
    }
}

fn doCheckpoint(self: *Self) !bool {
    var snapshot = self.acquireSegments();
    defer self.releaseSegments(&snapshot);

    const source = snapshot.memory_segments.value.getFirst() orelse return false;
    if (source.value.getSize() < self.options.min_segment_size) {
        return false;
    }

    // build new file segment

    var target = try FileSegmentList.createSegment(self.allocator, .{ .dir = self.data_dir });
    errdefer FileSegmentList.destroySegment(self.allocator, &target);

    var reader = source.value.reader();
    defer reader.close();

    try target.value.build(&reader);
    errdefer target.value.cleanup();

    // update memory segments list

    var memory_segments_update = try self.memory_segments.beginUpdate();
    defer self.memory_segments.cleanupAfterUpdate(&memory_segments_update);

    memory_segments_update.removeSegment(source);

    // update file segments list

    var file_segments_update = try self.file_segments.beginUpdate();
    defer self.file_segments.cleanupAfterUpdate(&file_segments_update);

    file_segments_update.appendSegment(target);

    try self.updateIndexFile(file_segments_update.segments.value);

    // commit updated lists

    self.segments_lock.lock();
    defer self.segments_lock.unlock();

    self.memory_segments.commitUpdate(&memory_segments_update);
    self.file_segments.commitUpdate(&file_segments_update);

    if (self.file_segments.needsMerge()) {
        self.file_segment_merge_event.set();
    }

    return true;
}

fn checkpointThreadFn(self: *Self) void {
    while (!self.stopping.load(.acquire)) {
        if (self.doCheckpoint()) |successful| {
            if (successful) {
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
    log.info("stopping checkpoint thread", .{});
    if (self.checkpoint_thread) |thread| {
        self.checkpoint_event.set();
        thread.join();
    }
    self.checkpoint_thread = null;
}

fn updateIndexFile(self: *Self, segments: *FileSegmentList) !void {
    var ids = try segments.getIds(self.allocator);
    defer ids.deinit(self.allocator);

    try filefmt.writeIndexFile(self.data_dir, ids.items);
}

fn maybeMergeFileSegments(self: *Self) !bool {
    var upd = try self.file_segments.prepareMerge() orelse return false;
    defer self.file_segments.cleanupAfterUpdate(&upd);

    try self.updateIndexFile(upd.segments.value);

    self.segments_lock.lock();
    defer self.segments_lock.unlock();

    self.file_segments.commitUpdate(&upd);

    return true;
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
    log.info("stopping file segment merge thread", .{});
    if (self.file_segment_merge_thread) |thread| {
        self.file_segment_merge_event.set();
        thread.join();
    }
    self.file_segment_merge_thread = null;
}

fn maybeMergeMemorySegments(self: *Self) !bool {
    var upd = try self.memory_segments.prepareMerge() orelse return false;
    defer self.memory_segments.cleanupAfterUpdate(&upd);

    self.segments_lock.lock();
    defer self.segments_lock.unlock();

    self.memory_segments.commitUpdate(&upd);

    self.maybeScheduleCheckpoint();

    return true;
}

fn memorySegmentMergeThreadFn(self: *Self) void {
    while (!self.stopping.load(.acquire)) {
        if (self.maybeMergeMemorySegments()) |successful| {
            if (successful) {
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
    log.info("stopping memory segment merge thread", .{});
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

fn maybeScheduleCheckpoint(self: *Self) void {
    if (self.memory_segments.segments.value.getFirst()) |first_node| {
        if (first_node.value.getSize() >= self.options.min_segment_size) {
            self.checkpoint_event.set();
        }
    }
}

fn readyForCheckpoint(self: *Self) ?MemorySegmentNode {
    self.segments_lock.lockShared();
    defer self.segments_lock.unlockShared();

    if (self.segments.memory_segments.value.getFirstOrNull()) |first_node| {
        if (first_node.value.getSize() > self.options.min_segment_size) {
            return first_node.acquire();
        }
    }
    return null;
}

pub fn update(self: *Self, changes: []const Change) !void {
    log.debug("update with {} changes", .{changes.len});

    var target = try MemorySegmentList.createSegment(self.allocator, .{});
    defer MemorySegmentList.destroySegment(self.allocator, &target);

    try target.value.build(changes);

    var upd = try self.memory_segments.beginUpdate();
    defer self.memory_segments.cleanupAfterUpdate(&upd);

    upd.appendSegment(target);

    try self.oplog.write(changes);

    self.segments_lock.lock();
    defer self.segments_lock.unlock();

    self.memory_segments.commitUpdate(&upd);

    if (self.memory_segments.needsMerge()) {
        self.memory_segment_merge_event.set();
    }
}

const SegmentsSnapshot = struct {
    file_segments: SharedPtr(FileSegmentList),
    memory_segments: SharedPtr(MemorySegmentList),
};

// Get the current segments lists and make sure they won't get deleted.
fn acquireSegments(self: *Self) SegmentsSnapshot {
    self.segments_lock.lockShared();
    defer self.segments_lock.unlockShared();

    return .{
        .file_segments = self.file_segments.segments.acquire(),
        .memory_segments = self.memory_segments.segments.acquire(),
    };
}

// Release the previously acquired segments lists, they will get deleted if no longer needed.
fn releaseSegments(self: *Self, segments: *SegmentsSnapshot) void {
    segments.file_segments.release(self.allocator, .{self.allocator});
    segments.memory_segments.release(self.allocator, .{self.allocator});
}

pub fn search(self: *Self, hashes: []const u32, allocator: std.mem.Allocator, deadline: Deadline) !SearchResults {
    const sorted_hashes = try allocator.dupe(u32, hashes);
    defer allocator.free(sorted_hashes);
    std.sort.pdq(u32, sorted_hashes, {}, std.sort.asc(u32));

    var results = SearchResults.init(allocator);
    errdefer results.deinit();

    var segments = self.acquireSegments();
    defer self.releaseSegments(&segments);

    try segments.file_segments.value.search(sorted_hashes, &results, deadline);
    try segments.memory_segments.value.search(sorted_hashes, &results, deadline);

    results.sort();
    return results;
}

pub fn getMaxCommitId(self: *Self) u64 {
    var segments = self.acquireSegments();
    defer self.releaseSegments(&segments);

    return @max(segments.file_segments.value.getMaxCommitId(), segments.memory_segments.value.getMaxCommitId());
}

test {
    // _ = @import("index_tests.zig");
    _ = @import("segment_list2.zig");
}
