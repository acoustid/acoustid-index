const std = @import("std");
const log = std.log.scoped(.index);

const zul = @import("zul");

const Deadline = @import("utils/Deadline.zig");
const Change = @import("change.zig").Change;
const SearchResult = @import("common.zig").SearchResult;
const SearchResults = @import("common.zig").SearchResults;
const SegmentID = @import("common.zig").SegmentID;

const Oplog = @import("Oplog.zig");

const MemorySegment = @import("InMemorySegment.zig");
const MemorySegmentList = MemorySegment.List;
const MemorySegmentNode = MemorySegment.List.List.Node;

const FileSegment = @import("FileSegment.zig");
const FileSegmentList = FileSegment.List;
const FileSegmentNode = FileSegment.List.List.Node;

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
segments_maintenance_lock: std.Thread.Mutex = .{},

// Mutex used to control linearity of updates.
update_lock: std.Thread.Mutex = .{},

// Mutex used to control merging of in-memory segments.
memory_merge_lock: std.Thread.Mutex = .{},

// Mutex used to control merging of file segments.
file_merge_lock: std.Thread.Mutex = .{},

stopping: std.atomic.Value(bool),

checkpoint_mutex: std.Thread.Mutex = .{},
checkpoint_condition: std.Thread.Condition = .{},
checkpoint_thread: ?std.Thread = null,
checkpoint_stop: bool = false,

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
        .memory_segments = MemorySegmentList.init(allocator),
        .stopping = std.atomic.Value(bool).init(false),
    };
}

pub fn deinit(self: *Self) void {
    self.stopping.store(true, .release);

    if (self.checkpoint_thread) |thread| {
        self.stopCheckpointThread();
        thread.join();
    }

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

fn finnishMemorySegmentMerge(self: *Self, merge: MemorySegmentList.PreparedMerge) bool {
    self.segments_lock.lock();
    defer self.segments_lock.unlock();

    self.memory_segments.applyMerge(merge);
    self.memory_segments.destroyMergedSegments(merge);

    self.flattenMemorySegmentIds();

    return merge.target.data.getSize() >= self.options.min_segment_size;
}

// Perform partial compaction on the in-memory segments.
fn maybeMergeMemorySegments(self: *Self) !bool {
    self.memory_merge_lock.lock();
    defer self.memory_merge_lock.unlock();

    const merge = try self.prepareMemorySegmentMerge() orelse return false;
    return self.finnishMemorySegmentMerge(merge);
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

    self.segments_maintenance_lock.lock();
    defer self.segments_maintenance_lock.unlock();

    var ids = try self.getFileSegmentIds();
    defer ids.deinit();

    try ids.append(dest.data.id);

    try filefmt.writeIndexFile(self.data_dir, ids);

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

fn checkpointThreadFn(self: *Self) void {
    const min_delay = std.time.ns_per_s;
    const max_delay = std.time.ns_per_s * 60;
    var delay: u64 = min_delay;
    var retries: u32 = 0;

    while (true) {
        self.checkpoint_mutex.lock();
        defer self.checkpoint_mutex.unlock();

        if (self.stopping.load(.acquire)) return;

        var wait: bool = false;

        if (self.doCheckpoint()) |successful| {
            delay = min_delay;
            retries = 0;
            wait = !successful;
        } else |err| {
            delay = @min(delay * 110 / 100, max_delay);
            retries += 1;
            wait = true;
            log.err("checkpoint failed: {} (retry {})", .{ err, retries });
        }

        if (wait) {
            self.checkpoint_condition.timedWait(&self.checkpoint_mutex, delay) catch {};
        }
    }
}

pub fn open(self: *Self) !void {
    try self.startCheckpointThread();
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

test "Index" {
    var tmpDir = std.testing.tmpDir(.{});
    defer tmpDir.cleanup();

    var index = try Self.init(std.testing.allocator, tmpDir.dir, .{ .create = true });
    defer index.deinit();

    try index.open();

    try index.update(&[_]Change{
        .{
            .insert = .{
                .id = 1,
                .hashes = &[_]u32{ 100, 101, 102 },
            },
        },
    });

    var results = try index.search(&[_]u32{ 100, 101, 102 }, std.testing.allocator, .{});
    defer results.deinit();

    try std.testing.expectEqual(1, results.count());
    try std.testing.expectEqualDeep(SearchResult{ .id = 1, .score = 3, .version = 1 }, results.get(1));
}
