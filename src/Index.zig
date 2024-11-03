const std = @import("std");
const log = std.log.scoped(.index);

const zul = @import("zul");

const InMemoryIndex = @import("InMemoryIndex.zig");

const common = @import("common.zig");
const SearchResults = common.SearchResults;
const Change = common.Change;
const SegmentID = common.SegmentID;

const Deadline = @import("utils/Deadline.zig");

const segment_list = @import("segment_list.zig");
const Segment = @import("Segment.zig");
const SegmentList = segment_list.SegmentList(Segment);

const Oplog = @import("Oplog.zig");

const filefmt = @import("filefmt.zig");

const Self = @This();

const Options = struct {
    min_segment_size: usize = 1000,
};

options: Options,

is_open: bool = false,

dir: std.fs.Dir,
allocator: std.mem.Allocator,
stage: InMemoryIndex,
segments: SegmentList,
write_lock: std.Thread.RwLock = .{},

scheduler: zul.Scheduler(Task, *Self),
last_cleanup_at: i64 = 0,
cleanup_interval: i64 = 1000,

max_segment_size: usize = 4 * 1024 * 1024 * 1024,

oplog: Oplog,
oplog_dir: std.fs.Dir,

const Task = union(enum) {
    cleanup: void,

    pub fn run(task: Task, index: *Self, at: i64) void {
        _ = at;
        switch (task) {
            .cleanup => {
                index.cleanup() catch |err| {
                    log.err("cleanup failed: {}", .{err});
                };
            },
        }
    }
};

pub fn init(allocator: std.mem.Allocator, dir: std.fs.Dir, options: Options) !Self {
    var done: bool = false;

    var oplog_dir = try dir.makeOpenPath("oplog", .{ .iterate = true });
    defer {
        if (!done) oplog_dir.close();
    }

    const self = Self{
        .options = options,
        .dir = dir,
        .allocator = allocator,
        .stage = InMemoryIndex.init(allocator, .{ .max_segment_size = options.min_segment_size }),
        .segments = SegmentList.init(allocator),
        .scheduler = zul.Scheduler(Task, *Self).init(allocator),
        .oplog = Oplog.init(allocator, oplog_dir),
        .oplog_dir = oplog_dir,
    };

    done = true;
    return self;
}

pub fn deinit(self: *Self) void {
    self.scheduler.deinit();
    self.oplog.deinit();
    self.oplog_dir.close();
    self.stage.deinit();
    self.segments.deinit();
}

pub fn open(self: *Self) !void {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    if (self.is_open) return;

    try self.scheduler.start(self);
    try self.readIndexFile();

    try self.oplog.open(self.segments.getMaxCommitId(), &self.stage);

    self.is_open = true;
}

const index_file_name = "index.dat";

fn writeIndexFile(self: *Self) !void {
    var file = try self.dir.atomicFile(index_file_name, .{});
    defer file.deinit();

    var ids = std.ArrayList(SegmentID).init(self.allocator);
    defer ids.deinit();

    try self.segments.getIds(&ids);

    try filefmt.writeIndexFile(file.file.writer(), ids);

    try file.finish();
}

fn readIndexFile(self: *Self) !void {
    if (self.segments.segments.len > 0) {
        return error.AlreadyOpened;
    }

    var file = self.dir.openFile(index_file_name, .{}) catch |err| {
        if (err == error.FileNotFound) {
            return;
        }
        return err;
    };
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

const PreparedMerge = struct {
    sources: SegmentList.SegmentsToMerge,
    target: *SegmentList.List.Node,
};

fn prepareMerge(self: *Self) !?PreparedMerge {
    self.write_lock.lockShared();
    defer self.write_lock.unlockShared();

    const sources_or_null = self.segments.findSegmentsToMerge(.{ .max_segment_size = self.max_segment_size });
    if (sources_or_null == null) {
        return null;
    }
    const sources = sources_or_null.?;

    var saved = false;

    var target = try self.segments.createSegment();
    defer {
        if (!saved) self.segments.destroySegment(target);
    }

    try target.data.merge(self.dir, .{ &sources.node1.data, &sources.node2.data });

    saved = true;
    return .{ .sources = sources, .target = target };
}

fn finnishMerge(self: *Self, merge: PreparedMerge) !void {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    var committed = false;

    defer {
        if (!committed) {
            merge.target.data.delete(self.dir) catch |err| {
                log.err("failed to delete segment: {}", .{err});
            };
            self.segments.destroySegment(merge.target);
        }
    }

    self.segments.segments.insertBefore(merge.sources.node1, merge.target);
    self.segments.segments.remove(merge.sources.node1);
    self.segments.segments.remove(merge.sources.node2);

    defer {
        if (!committed) {
            self.segments.segments.insertBefore(merge.target, merge.sources.node1);
            self.segments.segments.insertBefore(merge.target, merge.sources.node2);
            self.segments.segments.remove(merge.target);
        }
    }

    try self.writeIndexFile();

    committed = true;

    self.segments.destroySegment(merge.sources.node1);
    self.segments.destroySegment(merge.sources.node2);

    log.info("committed merge segment {}:{}", .{ merge.target.data.id.version, merge.target.data.id.included_merges });
}

fn compact(self: *Self) !void {
    while (true) {
        const merge_opt = try self.prepareMerge();
        if (merge_opt) |merge| {
            try self.finnishMerge(merge);
            return error.NotImplemented;
        }
        break;
    }
}

fn cleanup(self: *Self) !void {
    log.info("running cleanup", .{});

    var max_commit_id: ?u64 = null;

    if (self.stage.maybeFreezeOldestSegment()) |frozenStageSegment| {
        var committed = false;
        const node = try self.segments.createSegment();
        defer {
            if (!committed) {
                self.segments.destroySegment(node);
            }
        }

        try node.data.convert(self.dir, frozenStageSegment);

        self.write_lock.lock();
        defer self.write_lock.unlock();

        self.segments.segments.append(node);
        self.writeIndexFile() catch |err| {
            self.segments.segments.remove(node);
            node.data.delete(self.dir) catch {};
            return err;
        };

        self.stage.removeFrozenSegment(frozenStageSegment);
        max_commit_id = node.data.max_commit_id;
        committed = true;
    }

    if (max_commit_id) |commit_id| {
        self.oplog.truncate(commit_id) catch |err| {
            log.err("failed to truncate oplog: {}", .{err});
        };
    }

    try self.compact();
}

pub fn update(self: *Self, changes: []const Change) !void {
    self.write_lock.lockShared();
    defer self.write_lock.unlockShared();

    if (!self.is_open) {
        return error.NotOpened;
    }

    try self.oplog.write(changes, &self.stage);
    try self.scheduler.scheduleIn(.{ .cleanup = {} }, 0);
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

test "insert and search" {
    var tmpDir = std.testing.tmpDir(.{});
    defer tmpDir.cleanup();

    var index = try Self.init(std.testing.allocator, tmpDir.dir, .{});
    defer index.deinit();

    try index.open();

    try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }});

    var results = SearchResults.init(std.testing.allocator);
    defer results.deinit();

    try index.search(&[_]u32{ 1, 2, 3 }, &results, .{});

    try std.testing.expectEqual(1, results.count());

    const result = results.get(1);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(1, result.?.id);
    try std.testing.expectEqual(3, result.?.score);
    try std.testing.expect(result.?.version != 0);
}

test "persistance" {
    var tmpDir = std.testing.tmpDir(.{});
    defer tmpDir.cleanup();

    {
        var index = try Self.init(std.testing.allocator, tmpDir.dir, .{ .min_segment_size = 1000 });
        defer index.deinit();

        try index.open();

        var hashes_buf: [100]u32 = undefined;
        const hashes = hashes_buf[0..];
        var prng = std.rand.DefaultPrng.init(0);
        const rand = prng.random();
        for (0..100) |i| {
            for (hashes) |*h| {
                h.* = std.rand.int(rand, u32);
            }
            try index.update(&[_]Change{.{ .insert = .{
                .id = @intCast(i),
                .hashes = hashes,
            } }});
        }
    }

    {
        var index = try Self.init(std.testing.allocator, tmpDir.dir, .{});
        defer index.deinit();

        try index.open();
    }
}
