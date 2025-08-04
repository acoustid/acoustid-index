const std = @import("std");
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.index);

const zul = @import("zul");

const Deadline = @import("utils/Deadline.zig");
const Scheduler = @import("utils/Scheduler.zig");
const WaitGroup = @import("WaitGroup.zig").WaitGroup;
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
    max_concurrent_loads: u32 = 4,
    parallel_loading_threshold: usize = 3,
};

const MAX_CONCURRENT_LOADS = 4;

const SegmentLoadContext = struct {
    index: *Self,
    segment_info: SegmentInfo,
    result: *?FileSegmentList.Node,
    load_error: *?anyerror,
    wait_group: *WaitGroup,
    mutex: *std.Thread.Mutex,
};

const ParallelLoadState = struct {
    allocator: std.mem.Allocator,
    results: []?FileSegmentList.Node,
    errors: []?anyerror,
    tasks: []?Scheduler.Task,
    wait_group: WaitGroup,
    mutex: std.Thread.Mutex,
    
    fn init(allocator: std.mem.Allocator, segment_count: usize) !ParallelLoadState {
        const results = try allocator.alloc(?FileSegmentList.Node, segment_count);
        errdefer allocator.free(results);
        
        const errors = try allocator.alloc(?anyerror, segment_count);
        errdefer allocator.free(errors);
        
        const tasks = try allocator.alloc(?Scheduler.Task, segment_count);
        errdefer allocator.free(tasks);
        
        @memset(results, null);
        @memset(errors, null);
        @memset(tasks, null);
        
        return ParallelLoadState{
            .allocator = allocator,
            .results = results,
            .errors = errors,
            .tasks = tasks,
            .wait_group = WaitGroup.init(),
            .mutex = .{},
        };
    }
    
    fn deinit(self: *ParallelLoadState) void {
        // Clean up any remaining tasks
        for (self.tasks) |maybe_task| {
            if (maybe_task) |task| {
                // Tasks should be cleaned up by caller, but just in case
                _ = task;
            }
        }
        
        self.allocator.free(self.results);
        self.allocator.free(self.errors);
        self.allocator.free(self.tasks);
    }
};

options: Options,
allocator: std.mem.Allocator,
scheduler: *Scheduler,
name: []const u8,

dir: std.fs.Dir,

oplog: Oplog,

open_lock: std.Thread.Mutex = .{},
is_ready: std.Thread.ResetEvent = .{},
load_task: ?Scheduler.Task = null,

segments_lock: std.Thread.RwLock = .{},
memory_segments: SegmentListManager(MemorySegment),
file_segments: SegmentListManager(FileSegment),

checkpoint_task: ?Scheduler.Task = null,
file_segment_merge_task: ?Scheduler.Task = null,
memory_segment_merge_task: ?Scheduler.Task = null,

stopping: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
file_segment_merge_event: std.Thread.ResetEvent = .{},

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

    // Signal stopping to any background threads
    self.stopping.store(true, .release);
    self.file_segment_merge_event.set();

    if (self.load_task) |task| {
        self.scheduler.destroyTask(task);
    }

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

fn checkpoint(self: *Self) !bool {
    var source = self.memory_segments.prepareCheckpoint(self.allocator) orelse return false;
    defer MemorySegmentList.destroySegment(self.allocator, &source);

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
    var snapshot = self.acquireReader() catch return;
    defer self.releaseReader(&snapshot);

    metrics.docs(self.name, snapshot.getNumDocs());
}

fn checkpointTask(self: *Self) void {
    _ = self.checkpoint() catch |err| {
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
    if (self.is_ready.isSet()) {
        return;
    }

    self.open_lock.lock();
    defer self.open_lock.unlock();

    if (self.load_task != null) {
        return error.AlreadyOpening;
    }

    const manifest = filefmt.readManifestFile(self.dir, self.allocator) catch |err| {
        if (err == error.FileNotFound) {
            if (create) {
                try self.updateManifestFile(self.file_segments.segments.value);
                try self.load(&.{});
                return;
            }
            return error.IndexNotFound;
        }
        return err;
    };
    errdefer self.allocator.free(manifest);

    self.load_task = try self.scheduler.createTask(.medium, loadTask, .{ self, manifest });
    self.scheduler.scheduleTask(self.load_task.?);
}

fn loadSegmentTask(ctx: SegmentLoadContext) void {
    ctx.result.* = FileSegmentList.loadSegment(
        ctx.index.allocator,
        ctx.segment_info,
        .{ .dir = ctx.index.dir }
    ) catch |err| {
        // Store error without mutex - each task has its own error slot
        ctx.load_error.* = err;
        ctx.wait_group.done();
        return;
    };
    
    log.info("loaded segment {}", .{ctx.segment_info.version});
    ctx.wait_group.done();
}

fn load(self: *Self, manifest: []SegmentInfo) !void {
    defer self.allocator.free(manifest);

    log.info("found {} segments in manifest", .{manifest.len});

    if (manifest.len == 0) {
        return self.loadEmpty();
    }

    // Use parallel loading for manifests with 3+ segments
    if (manifest.len >= self.options.parallel_loading_threshold) {
        return self.loadParallel(manifest);
    } else {
        return self.loadSequential(manifest);
    }
}

fn loadEmpty(self: *Self) !void {
    self.memory_segment_merge_task = try self.scheduler.createTask(.high, memorySegmentMergeTask, .{self});
    self.checkpoint_task = try self.scheduler.createTask(.medium, checkpointTask, .{self});
    self.file_segment_merge_task = try self.scheduler.createTask(.low, fileSegmentMergeTask, .{self});

    try self.oplog.open(1, updateInternal, self);
    log.info("index loaded (empty)", .{});
    self.is_ready.set();
}

fn loadSequential(self: *Self, manifest: []SegmentInfo) !void {
    try self.file_segments.segments.value.nodes.ensureTotalCapacity(self.allocator, manifest.len);
    var last_commit_id: u64 = 0;
    
    for (manifest, 1..) |segment_info, i| {
        const node = try FileSegmentList.loadSegment(self.allocator, segment_info, .{ .dir = self.dir });
        self.file_segments.segments.value.nodes.appendAssumeCapacity(node);
        last_commit_id = node.value.info.getLastCommitId();
        log.info("loaded segment {} ({}/{})", .{ last_commit_id, i, manifest.len });
    }

    try self.completeLoading(last_commit_id);
}

fn loadParallel(self: *Self, manifest: []SegmentInfo) !void {
    log.info("using parallel loading for {} segments", .{manifest.len});
    
    var load_state = try ParallelLoadState.init(self.allocator, manifest.len);
    defer load_state.deinit();
    
    // Add all segments to the wait group
    load_state.wait_group.add(manifest.len);
    
    try self.file_segments.segments.value.nodes.ensureTotalCapacity(self.allocator, manifest.len);
    
    // Create and schedule loading tasks (with basic bounded concurrency)
    var active_tasks: usize = 0;
    for (manifest, 0..) |segment_info, i| {
        // Simple bounded concurrency - wait if we have too many active tasks
        while (active_tasks >= MAX_CONCURRENT_LOADS) {
            std.time.sleep(std.time.ns_per_ms);
            
            // Check if any tasks completed
            var completed_count: usize = 0;
            for (load_state.tasks[0..i]) |maybe_task| {
                if (maybe_task) |task| {
                    // Check if task is done (non-blocking)
                    if (!task.data.running and !task.data.scheduled) {
                        completed_count += 1;
                    }
                }
            }
            if (completed_count > 0) {
                active_tasks = @min(active_tasks, MAX_CONCURRENT_LOADS - 1);
                break;
            }
        }
        
        const load_context = SegmentLoadContext{
            .index = self,
            .segment_info = segment_info,
            .result = &load_state.results[i],
            .load_error = &load_state.errors[i],
            .wait_group = &load_state.wait_group,
            .mutex = &load_state.mutex,
        };
        
        load_state.tasks[i] = self.scheduler.createTask(.high, loadSegmentTask, .{load_context}) catch |err| {
            load_state.errors[i] = err;
            load_state.wait_group.done();
            continue;
        };
        
        self.scheduler.scheduleTask(load_state.tasks[i].?);
        active_tasks += 1;
    }
    
    // Wait for all tasks to complete
    load_state.wait_group.wait();
    
    // Clean up tasks
    for (load_state.tasks) |maybe_task| {
        if (maybe_task) |task| {
            self.scheduler.destroyTask(task);
        }
    }
    
    // Process results and handle errors
    var last_commit_id: u64 = 0;
    var any_errors = false;
    
    for (load_state.results, load_state.errors, 0..) |maybe_node, maybe_error, i| {
        if (maybe_error) |err| {
            log.err("failed to load segment {}: {}", .{manifest[i].version, err});
            any_errors = true;
            continue;
        }
        
        if (maybe_node) |node| {
            self.file_segments.segments.value.nodes.appendAssumeCapacity(node);
            last_commit_id = @max(last_commit_id, node.value.info.getLastCommitId());
        } else {
            log.err("segment {} loaded but no result or error", .{manifest[i].version});
            any_errors = true;
        }
    }
    
    if (any_errors) {
        return error.SegmentLoadFailed;
    }
    
    log.info("parallel loading completed: {} segments", .{manifest.len});
    try self.completeLoading(last_commit_id);
}

fn completeLoading(self: *Self, last_commit_id: u64) !void {
    self.memory_segment_merge_task = try self.scheduler.createTask(.high, memorySegmentMergeTask, .{self});
    self.checkpoint_task = try self.scheduler.createTask(.medium, checkpointTask, .{self});
    self.file_segment_merge_task = try self.scheduler.createTask(.low, fileSegmentMergeTask, .{self});

    try self.oplog.open(last_commit_id + 1, updateInternal, self);

    log.info("index loaded", .{});
    self.is_ready.set();
}

fn loadTask(self: *Self, manifest: []SegmentInfo) void {
    self.open_lock.lock();
    defer self.open_lock.unlock();

    self.load(manifest) catch |err| {
        log.err("load failed: {}", .{err});
    };
}

fn maybeScheduleMemorySegmentMerge(self: *Self) void {
    if (self.memory_segments.needsMerge()) {
        if (self.memory_segment_merge_task) |task| {
            log.debug("too many memory segments, scheduling merging", .{});
            self.scheduler.scheduleTask(task);
        }
    }
}

fn maybeScheduleFileSegmentMerge(self: *Self) void {
    if (self.file_segments.needsMerge()) {
        if (self.file_segment_merge_task) |task| {
            log.debug("too many file segments, scheduling merging", .{});
            self.scheduler.scheduleTask(task);
        }
    }
}

fn maybeScheduleCheckpoint(self: *Self) void {
    if (self.memory_segments.segments.value.getFirst()) |first_node| {
        if (first_node.value.getSize() >= self.options.min_segment_size) {
            if (self.checkpoint_task) |task| {
                log.debug("the first memory segment is too big, scheduling checkpoint", .{});
                self.scheduler.scheduleTask(task);
            }
        }
    }
}

pub fn waitForReady(self: *Self, timeout_ms: u32) !void {
    try self.is_ready.timedWait(@as(u64, timeout_ms) * std.time.ns_per_ms);
}

pub fn checkReady(self: *Self) !void {
    if (!self.is_ready.isSet()) {
        return error.IndexNotReady;
    }
}

pub fn update(self: *Self, changes: []const Change) !void {
    try self.checkReady();
    try self.updateInternal(changes, null);
}

fn updateInternal(self: *Self, changes: []const Change, commit_id: ?u64) !void {
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

pub fn acquireReader(self: *Self) !IndexReader {
    try self.checkReady();

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

pub fn search(self: *Self, hashes: []u32, results: *SearchResults, deadline: Deadline) !void {
    var reader = try self.acquireReader();
    defer self.releaseReader(&reader);

    try reader.search(hashes, results, deadline);
}

test {
    _ = @import("index_tests.zig");
}
