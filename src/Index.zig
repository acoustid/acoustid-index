const std = @import("std");
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.index);

const zul = @import("zul");

const Deadline = @import("utils/Deadline.zig");
const Scheduler = @import("utils/Scheduler.zig");
const Change = @import("change.zig").Change;
const SearchResult = @import("common.zig").SearchResult;
const SearchResults = @import("common.zig").SearchResults;
const SegmentInfo = @import("segment.zig").SegmentInfo;
const DocInfo = @import("common.zig").DocInfo;

const Oplog = @import("Oplog.zig");

const SegmentList = @import("segment_list.zig").SegmentList;
const SegmentListManager = @import("segment_list.zig").SegmentListManager;

const MemorySegment = @import("MemorySegment.zig");
const MemorySegmentList = SegmentList(MemorySegment);
const MemorySegmentNode = MemorySegmentList.Node;

const FileSegment = @import("FileSegment.zig");
const FileSegmentList = SegmentList(FileSegment);
const FileSegmentNode = FileSegmentList.Node;

const IndexReader = @import("IndexReader.zig");
const SharedPtr = @import("utils/shared_ptr.zig").SharedPtr;

const SegmentMerger = @import("segment_merger.zig").SegmentMerger;

const TieredMergePolicy = @import("segment_merge_policy.zig").TieredMergePolicy;

const filefmt = @import("filefmt.zig");

const metrics = @import("metrics.zig");
const Self = @This();

const Options = struct {
    min_segment_size: usize = 500_000,
    max_segment_size: usize = 750_000_000,
};

options: Options,
allocator: std.mem.Allocator,
scheduler: *Scheduler,
name: []const u8,

dir: std.fs.Dir,

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

checkpoint_task: ?Scheduler.Task = null,
file_segment_merge_task: ?Scheduler.Task = null,
memory_segment_merge_task: ?Scheduler.Task = null,

fn getFileSegmentSize(segment: SharedPtr(FileSegment)) usize {
    return segment.value.getSize();
}

fn getMemorySegmentSize(segment: SharedPtr(MemorySegment)) usize {
    return segment.value.getSize();
}

pub fn init(allocator: std.mem.Allocator, scheduler: *Scheduler, parent_dir: std.fs.Dir, path: []const u8, options: Options) !Self {
    var dir = try parent_dir.makeOpenPath(path, .{ .iterate = true });
    errdefer dir.close();

    var oplog = try Oplog.init(allocator, dir);
    errdefer oplog.deinit();

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
            .dir = dir,
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
        .scheduler = scheduler,
        .dir = dir,
        .name = path,
        .oplog = oplog,
        .segments_lock = .{},
        .memory_segments = memory_segments,
        .file_segments = file_segments,
    };
}

pub fn deinit(self: *Self) void {
    log.info("closing index {}", .{@intFromPtr(self)});

    if (self.checkpoint_task) |task| {
        self.scheduler.destroyTask(task);
    }

    if (self.memory_segment_merge_task) |task| {
        self.scheduler.destroyTask(task);
    }

    if (self.file_segment_merge_task) |task| {
        self.scheduler.destroyTask(task);
    }

    self.memory_segments.deinit(self.allocator, .keep);
    self.file_segments.deinit(self.allocator, .keep);

    self.oplog.deinit();
    self.dir.close();
}

fn loadSegments(self: *Self, create: bool) !u64 {
    self.segments_lock.lock();
    defer self.segments_lock.unlock();

    const segment_ids = filefmt.readManifestFile(self.dir, self.allocator) catch |err| {
        if (err == error.FileNotFound) {
            if (create) {
                try self.updateManifestFile(self.file_segments.segments.value);
                return 0;
            }
            return error.IndexNotFound;
        }
        return err;
    };
    defer self.allocator.free(segment_ids);
    log.info("found {} segments in manifest", .{segment_ids.len});

    try self.file_segments.segments.value.nodes.ensureTotalCapacity(self.allocator, segment_ids.len);
    var last_commit_id: u64 = 0;
    for (segment_ids, 1..) |segment_id, i| {
        const node = try FileSegmentList.loadSegment(self.allocator, segment_id, .{ .dir = self.dir });
        self.file_segments.segments.value.nodes.appendAssumeCapacity(node);
        last_commit_id = node.value.info.getLastCommitId();
        log.info("loaded segment {} ({}/{})", .{ last_commit_id, i, segment_ids.len });
    }
    return last_commit_id;
}

fn doCheckpoint(self: *Self) !bool {
    var snapshot = self.acquireReader();
    defer self.releaseReader(&snapshot);

    const source = snapshot.memory_segments.value.getFirst() orelse return false;
    if (source.value.getSize() < self.options.min_segment_size) {
        return false;
    }

    // build new file segment

    var target = try FileSegmentList.createSegment(self.allocator, .{ .dir = self.dir });
    defer FileSegmentList.destroySegment(self.allocator, &target);

    var reader = source.value.reader();
    defer reader.close();

    try target.value.build(&reader);
    errdefer target.value.cleanup();

    // update memory segments list

    var memory_segments_update = try self.memory_segments.beginUpdate(self.allocator);
    defer self.memory_segments.cleanupAfterUpdate(self.allocator, &memory_segments_update);

    memory_segments_update.removeSegment(source);

    // update file segments list

    var file_segments_update = try self.file_segments.beginUpdate(self.allocator);
    defer self.file_segments.cleanupAfterUpdate(self.allocator, &file_segments_update);

    file_segments_update.appendSegment(target);

    try self.updateManifestFile(file_segments_update.segments.value);

    defer self.oplog.truncate(target.value.info.getLastCommitId()) catch |err| {
        log.warn("failed to truncate oplog: {}", .{err});
    };

    defer self.updateDocsMetrics();

    // commit updated lists

    self.segments_lock.lock();
    defer self.segments_lock.unlock();

    self.memory_segments.commitUpdate(&memory_segments_update);
    self.file_segments.commitUpdate(&file_segments_update);

    metrics.checkpoint();

    self.maybeScheduleFileSegmentMerge();

    return true;
}

fn updateDocsMetrics(self: *Self) void {
    var snapshot = self.acquireReader();
    defer self.releaseReader(&snapshot);

    metrics.docs(self.name, snapshot.getNumDocs());
}

fn checkpointTask(self: *Self) void {
    _ = self.doCheckpoint() catch |err| {
        log.err("checkpoint failed: {}", .{err});
    };
}

fn memorySegmentMergeTask(self: *Self) void {
    _ = self.maybeMergeMemorySegments() catch |err| {
        log.err("memory segment merge failed: {}", .{err});
    };
}

fn fileSegmentMergeTask(self: *Self) void {
    _ = self.maybeMergeFileSegments() catch |err| {
        log.err("file segment merge failed: {}", .{err});
    };
}

fn updateManifestFile(self: *Self, segments: *FileSegmentList) !void {
    const infos = try self.allocator.alloc(SegmentInfo, segments.nodes.items.len);
    defer self.allocator.free(infos);

    for (segments.nodes.items, 0..) |node, i| {
        infos[i] = node.value.info;
    }

    try filefmt.writeManifestFile(self.dir, infos);
}

fn maybeMergeFileSegments(self: *Self) !bool {
    var upd = try self.file_segments.prepareMerge(self.allocator) orelse return false;
    defer self.file_segments.cleanupAfterUpdate(self.allocator, &upd);

    try self.updateManifestFile(upd.segments.value);

    defer self.updateDocsMetrics();

    self.segments_lock.lock();
    defer self.segments_lock.unlock();

    self.file_segments.commitUpdate(&upd);

    metrics.fileSegmentMerge();

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

fn maybeMergeMemorySegments(self: *Self) !bool {
    var upd = try self.memory_segments.prepareMerge(self.allocator) orelse return false;
    defer self.memory_segments.cleanupAfterUpdate(self.allocator, &upd);

    defer self.updateDocsMetrics();

    self.segments_lock.lock();
    defer self.segments_lock.unlock();

    self.memory_segments.commitUpdate(&upd);

    metrics.memorySegmentMerge();

    self.maybeScheduleCheckpoint();

    return true;
}

pub fn open(self: *Self, create: bool) !void {
    const last_commit_id = try self.loadSegments(create);

    self.checkpoint_task = try self.scheduler.createTask(.medium, checkpointTask, self);
    self.memory_segment_merge_task = try self.scheduler.createTask(.high, memorySegmentMergeTask, self);
    self.file_segment_merge_task = try self.scheduler.createTask(.low, fileSegmentMergeTask, self);

    try self.oplog.open(last_commit_id + 1, updateInternal, self);

    log.info("index loaded", .{});
}

fn maybeScheduleMemorySegmentMerge(self: *Self) void {
    if (self.memory_segments.needsMerge()) {
        log.debug("too many memory segments, scheduling merging", .{});
        if (self.memory_segment_merge_task) |task| {
            self.scheduler.scheduleTask(task);
        }
    }
}

fn maybeScheduleFileSegmentMerge(self: *Self) void {
    if (self.file_segments.needsMerge()) {
        log.debug("too many file segments, scheduling merging", .{});
        if (self.file_segment_merge_task) |task| {
            self.scheduler.scheduleTask(task);
        }
    }
}

fn maybeScheduleCheckpoint(self: *Self) void {
    if (self.memory_segments.segments.value.getFirst()) |first_node| {
        log.debug("the first memory segment is too big, scheduling checkpoint", .{});
        if (first_node.value.getSize() >= self.options.min_segment_size) {
            if (self.checkpoint_task) |task| {
                self.scheduler.scheduleTask(task);
            }
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
    try self.updateInternal(changes, null);
}

pub fn updateInternal(self: *Self, changes: []const Change, commit_id: ?u64) !void {
    var target = try MemorySegmentList.createSegment(self.allocator, .{});
    defer MemorySegmentList.destroySegment(self.allocator, &target);

    try target.value.build(changes);

    var upd = try self.memory_segments.beginUpdate(self.allocator);
    defer self.memory_segments.cleanupAfterUpdate(self.allocator, &upd);

    target.value.info.version = commit_id orelse try self.oplog.write(changes);

    defer self.updateDocsMetrics();

    self.segments_lock.lock();
    defer self.segments_lock.unlock();

    upd.appendSegment(target);

    self.memory_segments.commitUpdate(&upd);

    self.maybeScheduleMemorySegmentMerge();
    self.maybeScheduleCheckpoint();
}

pub fn acquireReader(self: *Self) IndexReader {
    self.segments_lock.lockShared();
    defer self.segments_lock.unlockShared();

    return IndexReader{
        .file_segments = self.file_segments.segments.acquire(),
        .memory_segments = self.memory_segments.segments.acquire(),
    };
}

pub fn releaseReader(self: *Self, reader: *IndexReader) void {
    MemorySegmentList.destroySegments(self.allocator, &reader.memory_segments);
    FileSegmentList.destroySegments(self.allocator, &reader.file_segments);
}

pub fn search(self: *Self, hashes: []const u32, allocator: std.mem.Allocator, deadline: Deadline) !SearchResults {
    var reader = self.acquireReader();
    defer self.releaseReader(&reader);

    return reader.search(hashes, allocator, deadline);
}

test {
    _ = @import("index_tests.zig");
}
