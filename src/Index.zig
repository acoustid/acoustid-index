const std = @import("std");
const log = std.log.scoped(.index);

const InMemoryIndex = @import("InMemoryIndex.zig");

const common = @import("common.zig");
const SearchResults = common.SearchResults;
const Change = common.Change;
const SegmentID = common.SegmentID;

const Deadline = @import("utils/Deadline.zig");
const Scheduler = @import("utils/Scheduler.zig");

const Segment = @import("Segment.zig");
const SegmentList = Segment.List;

const Oplog = @import("Oplog.zig");

const filefmt = @import("filefmt.zig");

const Self = @This();

const Options = struct {
    create: bool = false,
    min_segment_size: usize = 1000,
};

options: Options,

is_open: bool = false,

allocator: std.mem.Allocator,
dir: std.fs.Dir,
scheduler: *Scheduler,
stage: InMemoryIndex,
segments: SegmentList,

write_lock: std.Thread.RwLock = .{},

cleanup_stop: std.atomic.Value(bool),
cleanup_thread: ?std.Thread = null,
cleanup_last_run_at: i64 = 0,
cleanup_interval: i64 = 1000,

max_segment_size: usize = 4 * 1024 * 1024 * 1024,

oplog: Oplog,
oplog_dir: std.fs.Dir,

pub fn init(allocator: std.mem.Allocator, dir: std.fs.Dir, scheduler: *Scheduler, options: Options) !Self {
    var oplog_dir = try dir.makeOpenPath("oplog", .{ .iterate = true });
    errdefer oplog_dir.close();

    return .{
        .options = options,
        .allocator = allocator,
        .dir = dir,
        .scheduler = scheduler,
        .stage = InMemoryIndex.init(allocator, .{ .max_segment_size = options.min_segment_size }),
        .segments = SegmentList.init(allocator),
        .oplog = Oplog.init(allocator, oplog_dir),
        .oplog_dir = oplog_dir,
        .cleanup_stop = std.atomic.Value(bool).init(false),
    };
}

pub fn deinit(self: *Self) void {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    if (self.cleanup_thread) |thread| {
        self.cleanup_stop.store(true, .monotonic);
        log.debug("waiting for cleanup thread to stop", .{});
        thread.join();
        self.cleanup_thread = null;
    }

    self.oplog.deinit();
    self.oplog_dir.close();

    self.stage.deinit();
    self.segments.deinit();
}

fn startCleanupThread(self: *Self) void {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    if (self.cleanup_thread == null) {
        log.info("starting cleanup thread", .{});
        self.cleanup_stop.store(false, .monotonic);
        self.cleanup_thread = std.Thread.spawn(.{}, cleanup, .{self}) catch |err| {
            log.err("failed to start cleanup thread: {}", .{err});
            return;
        };
    }
}

pub fn open(self: *Self) !void {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    if (self.is_open) return;

    self.readIndexFile() catch |err| {
        if (err == error.FileNotFound and self.options.create) {
            try self.writeIndexFile();
        } else {
            return err;
        }
    };

    try self.oplog.open(self.segments.getMaxCommitId(), &self.stage);

    self.is_open = true;
}

fn writeIndexFile(self: *Self) !void {
    var file = try self.dir.atomicFile(filefmt.index_file_name, .{});
    defer file.deinit();

    var ids = std.ArrayList(SegmentID).init(self.allocator);
    defer ids.deinit();

    try self.segments.getIds(&ids);

    try filefmt.writeIndexFile(file.file.writer(), ids);

    try file.finish();
}

fn readIndexFile(self: *Self) !void {
    var file = try self.dir.openFile(filefmt.index_file_name, .{});
    defer file.close();

    var ids = std.ArrayList(SegmentID).init(self.allocator);
    defer ids.deinit();

    try filefmt.readIndexFile(file.reader(), &ids);

    for (ids.items) |id| {
        const node = try self.segments.createSegment();
        try node.data.open(self.dir, id);
        self.segments.segments.append(node);
    }
}

fn prepareMerge(self: *Self) !?SegmentList.PreparedMerge {
    self.write_lock.lockShared();
    defer self.write_lock.unlockShared();

    const merge_opt = try self.segments.prepareMerge(.{ .max_segment_size = self.max_segment_size });
    if (merge_opt) |merge| {
        errdefer self.segments.destroySegment(merge.target);
        try merge.target.data.merge(self.dir, merge.sources, self.segments);
        return merge;
    }
    return null;
}

fn finnishMerge(self: *Self, merge: SegmentList.PreparedMerge) !void {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    errdefer self.segments.destroySegment(merge.target);

    errdefer merge.target.data.delete(self.dir);

    self.segments.applyMerge(merge);
    errdefer self.segments.revertMerge(merge);

    try self.writeIndexFile();

    self.segments.destroyMergedSegments(merge);

    log.info("committed merge segment {}:{}", .{ merge.target.data.id.version, merge.target.data.id.included_merges });
}

fn maybeMergeSegments(self: *Self) !void {
    while (true) {
        const merge_opt = try self.prepareMerge();
        if (merge_opt) |merge| {
            try self.finnishMerge(merge);
        } else {
            break;
        }
    }
}

fn maybeWriteNewSegment(self: *Self) !void {
    const source_segment = self.stage.maybeFreezeOldestSegment() orelse return;

    const node = try self.segments.createSegment();
    errdefer self.segments.destroySegment(node);

    try node.data.convert(self.dir, source_segment);

    errdefer node.data.delete(self.dir);

    self.write_lock.lock();
    defer self.write_lock.unlock();

    self.segments.segments.append(node);
    errdefer self.segments.segments.remove(node);

    try self.writeIndexFile();

    self.stage.removeFrozenSegment(source_segment);
}

fn truncateOplog(self: *Self) !void {
    self.write_lock.lockShared();
    defer self.write_lock.unlockShared();

    const max_commit_id = self.segments.getMaxCommitId();
    try self.oplog.truncate(max_commit_id);
}

fn cleanup(self: *Self) void {
    while (!self.cleanup_stop.load(.monotonic)) {
        const now = std.time.milliTimestamp();
        if (self.cleanup_last_run_at + self.cleanup_interval > now) {
            std.time.sleep(std.time.ns_per_ms * 100);
            continue;
        }
        self.cleanup_last_run_at = now;

        log.info("running cleanup", .{});

        self.maybeWriteNewSegment() catch |err| {
            log.warn("failed to write new segment: {}", .{err});
        };

        self.maybeMergeSegments() catch |err| {
            log.warn("failed to merge segments: {}", .{err});
        };

        self.truncateOplog() catch |err| {
            log.warn("failed to truncate oplog: {}", .{err});
        };
    }
}

fn applyChanges(self: *Self, changes: []const Change) !void {
    self.write_lock.lockShared();
    defer self.write_lock.unlockShared();

    try self.oplog.write(changes, &self.stage);
}

pub fn update(self: *Self, changes: []const Change) !void {
    self.startCleanupThread();
    try self.applyChanges(changes);
}

pub fn search(self: *Self, hashes: []const u32, results: *SearchResults, deadline: Deadline) !void {
    self.write_lock.lockShared();
    defer self.write_lock.unlockShared();

    if (!self.is_open) {
        return error.NotOpened;
    }

    const sorted_hashes = try self.allocator.dupe(u32, hashes);
    defer self.allocator.free(sorted_hashes);
    std.sort.pdq(u32, sorted_hashes, {}, std.sort.asc(u32));

    try self.segments.search(sorted_hashes, results, deadline);
    try self.stage.search(sorted_hashes, results, deadline);

    results.sort();
}

test {
    _ = @import("index_tests.zig");
}
