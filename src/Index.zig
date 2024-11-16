const std = @import("std");
const log = std.log.scoped(.index);

const common = @import("common.zig");
const SearchResults = common.SearchResults;
const SegmentID = common.SegmentID;

const Change = @import("change.zig").Change;

const Deadline = @import("utils/Deadline.zig");
const Scheduler = @import("utils/Scheduler.zig");

const InMemoryIndex = @import("InMemoryIndex.zig");
const FileIndex = @import("FileIndex.zig");
const Oplog = @import("Oplog.zig");

const Self = @This();

const Options = struct {
    create: bool = false,
    min_segment_size: usize = 1_000_000,
    max_segment_size: usize = 100_000_000,
};

options: Options,

allocator: std.mem.Allocator,
dir: std.fs.Dir,
scheduler: *Scheduler,

lock_file: ?std.fs.File = null,

stage: InMemoryIndex,

index: FileIndex,
index_dir: std.fs.Dir,

oplog: Oplog,
oplog_dir: std.fs.Dir,

checkpoint_task_id: ?u64 = null,
checkpoint_lock: std.Thread.Mutex = .{},

merge_task_id: ?u64 = null,
merge_lock: std.Thread.Mutex = .{},

pub fn init(allocator: std.mem.Allocator, dir: std.fs.Dir, scheduler: *Scheduler, options: Options) !Self {
    var index_dir = try dir.makeOpenPath("index", .{ .iterate = true });
    errdefer index_dir.close();

    var oplog_dir = try dir.makeOpenPath("oplog", .{ .iterate = true });
    errdefer oplog_dir.close();

    return .{
        .options = options,
        .allocator = allocator,
        .dir = dir,
        .scheduler = scheduler,
        .stage = InMemoryIndex.init(
            allocator,
            .{
                .max_segment_size = options.min_segment_size,
            },
        ),
        .index = FileIndex.init(
            allocator,
            index_dir,
            scheduler,
            .{
                .create = options.create,
                .min_segment_size = options.min_segment_size,
                .max_segment_size = options.max_segment_size,
            },
        ),
        .index_dir = index_dir,
        .oplog = Oplog.init(allocator, oplog_dir),
        .oplog_dir = oplog_dir,
    };
}

pub fn deinit(self: *Self) void {
    self.scheduler.cancelByContext(self);

    self.oplog.deinit();
    self.oplog_dir.close();

    self.index.deinit();
    self.index_dir.close();

    self.stage.deinit();

    if (self.lock_file) |file| {
        file.close();
        self.lock_file = null;
    }
}

pub fn open(self: *Self) !void {
    self.lock_file = self.dir.createFile(".lock", .{ .lock = .exclusive, .lock_nonblocking = true }) catch |err| {
        if (err == error.WouldBlock) {
            return error.LockedByAnotherProcess;
        }
        return err;
    };

    try self.index.open();
    try self.oplog.open(self.index.getMaxCommitId(), &self.stage);
}

fn scheduleCheckpoint(self: *Self) void {
    if (self.checkpoint_lock.tryLock()) {
        defer self.checkpoint_lock.unlock();
        if (self.checkpoint_task_id == null) {
            const task_id = self.scheduler.schedule(runCheckpoint, self, .{}) catch |err| {
                log.err("failed to schedule checkpoint: {}", .{err});
                return;
            };
            self.checkpoint_task_id = task_id;
        }
    }
}

fn scheduleMerge(self: *Self) void {
    if (self.merge_lock.tryLock()) {
        defer self.merge_lock.unlock();
        if (self.merge_task_id == null) {
            const task_id = self.scheduler.schedule(runMerge, self, .{}) catch |err| {
                log.err("failed to schedule merge: {}", .{err});
                return;
            };
            self.merge_task_id = task_id;
        }
    }
}

fn runCheckpoint(self: *Self) void {
    self.checkpoint_lock.lock();
    defer self.checkpoint_lock.unlock();

    self.checkpoint() catch |err| {
        log.err("failed to run checkpoint: {}", .{err});
    };

    self.checkpoint_task_id = null;
}

fn runMerge(self: *Self) void {
    self.merge_lock.lock();
    defer self.merge_lock.unlock();

    self.merge() catch |err| {
        log.err("failed to run merge: {}", .{err});
    };

    self.merge_task_id = null;
}

fn checkpoint(self: *Self) !void {
    const result = try self.index.checkpoint(&self.stage);
    if (result) |info| {
        log.info("checkpoint: max_commit_id={}", .{info.max_commit_id});
        try self.oplog.truncate(info.max_commit_id);
        self.scheduleMerge();
    }
}

fn merge(self: *Self) !void {
    try self.index.maybeMergeSegments();
}

pub fn update(self: *Self, changes: []const Change) !void {
    try self.oplog.write(changes, &self.stage);
    if (self.stage.isReadyForCheckpoint()) {
        self.scheduleCheckpoint();
    }
}

pub fn getMaxCommitId(self: *Self) u64 {
    return @max(self.index.getMaxCommitId(), self.stage.getMaxCommitId());
}

pub fn search(self: *Self, hashes: []const u32, results: *SearchResults, deadline: Deadline) !void {
    const sorted_hashes = try self.allocator.dupe(u32, hashes);
    defer self.allocator.free(sorted_hashes);
    std.sort.pdq(u32, sorted_hashes, {}, std.sort.asc(u32));

    try self.index.search(sorted_hashes, results, deadline);
    try self.stage.search(sorted_hashes, results, deadline);

    results.sort();
}

test {
    _ = @import("index_tests.zig");
}
