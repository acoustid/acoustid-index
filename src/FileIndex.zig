const std = @import("std");
const log = std.log.scoped(.index);

const InMemoryIndex = @import("InMemoryIndex.zig");

const common = @import("common.zig");
const SearchResults = common.SearchResults;
const SegmentID = common.SegmentID;

const Change = @import("change.zig").Change;

const Deadline = @import("utils/Deadline.zig");
const Scheduler = @import("utils/Scheduler.zig");

const FileSegment = @import("FileSegment.zig");
const FileSegmentList = FileSegment.List;

const SegmentMerger = @import("segment_merger.zig").SegmentMerger;

const Oplog = @import("Oplog.zig");

const filefmt = @import("filefmt.zig");

const Self = @This();

const Options = struct {
    create: bool = false,
    min_segment_size: usize = 1_000,
    max_segment_size: usize = 100_000_000,
};

options: Options,

allocator: std.mem.Allocator,
dir: std.fs.Dir,
scheduler: *Scheduler,
segments: FileSegmentList,

write_lock: std.Thread.RwLock = .{},

pub fn init(allocator: std.mem.Allocator, dir: std.fs.Dir, scheduler: *Scheduler, options: Options) Self {
    return .{
        .allocator = allocator,
        .dir = dir,
        .scheduler = scheduler,
        .options = options,
        .segments = FileSegmentList.init(allocator),
    };
}

pub fn deinit(self: *Self) void {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    self.scheduler.cancelByContext(self);

    self.segments.deinit();
}

pub fn open(self: *Self) !void {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    self.readIndexFile() catch |err| {
        if (err == error.FileNotFound and self.options.create) {
            try self.writeIndexFile();
        } else {
            return err;
        }
    };
}

fn prepareIndexFile(self: *Self, segments: []SegmentID) !std.fs.AtomicFile {
    var file = try self.dir.atomicFile(filefmt.index_file_name, .{});
    errdefer file.deinit();

    try filefmt.writeIndexFile(file.file.writer(), segments);
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

fn prepareMerge(self: *Self) !?FileSegmentList.PreparedMerge {
    self.write_lock.lockShared();
    defer self.write_lock.unlockShared();

    const merge = try self.segments.prepareMerge(.{ .max_segment_size = self.options.max_segment_size }) orelse return null;
    errdefer self.segments.destroySegment(merge.target);

    var merger = SegmentMerger(FileSegment).init(self.allocator, &self.segments);
    defer merger.deinit();

    try merger.addSource(&merge.sources.node1.data);
    try merger.addSource(&merge.sources.node2.data);
    try merger.prepare();

    try merge.target.data.build(self.dir, &merger);

    return merge;
}

fn finnishMerge(self: *Self, merge: FileSegmentList.PreparedMerge) !void {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    errdefer self.segments.destroySegment(merge.target);

    errdefer merge.target.data.delete(self.dir);

    self.segments.applyMerge(merge);
    errdefer self.segments.revertMerge(merge);

    try self.writeIndexFile();

    merge.sources.node1.data.delete(self.dir);
    merge.sources.node2.data.delete(self.dir);

    self.segments.destroyMergedSegments(merge);

    log.info("committed merge segment {}:{}", .{ merge.target.data.id.version, merge.target.data.id.included_merges });
}

pub fn maybeMergeSegments(self: *Self) !void {
    while (true) {
        if (try self.prepareMerge()) |merge| {
            try self.finnishMerge(merge);
        } else {
            break;
        }
    }
}

pub const CheckpointInfo = struct {
    max_commit_id: u64,
};

pub fn checkpoint(self: *Self, stage: *InMemoryIndex) !?CheckpointInfo {
    const source_segment = stage.maybeFreezeOldestSegment() orelse return null;

    var source_reader = source_segment.reader();
    defer source_reader.close();

    const node = try self.segments.createSegment();
    errdefer self.segments.destroySegment(node);

    try node.data.build(self.dir, &source_reader);

    errdefer node.data.delete(self.dir);

    self.write_lock.lock();
    defer self.write_lock.unlock();

    self.segments.segments.append(node);
    errdefer self.segments.segments.remove(node);

    try self.writeIndexFile();

    stage.removeFrozenSegment(source_segment);

    return .{ .max_commit_id = self.segments.getMaxCommitId() };
}

pub fn getMaxCommitId(self: *Self) u64 {
    self.write_lock.lockShared();
    defer self.write_lock.unlockShared();

    return self.segments.getMaxCommitId();
}

pub fn search(self: *Self, sorted_hashes: []const u32, results: *SearchResults, deadline: Deadline) !void {
    self.write_lock.lockShared();
    defer self.write_lock.unlockShared();

    try self.segments.search(sorted_hashes, results, deadline);
}

test {
    _ = @import("index_tests.zig");
}
