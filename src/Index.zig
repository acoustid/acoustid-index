const std = @import("std");
const log = std.log.scoped(.index);

const zul = @import("zul");

const InMemoryIndex = @import("InMemoryIndex.zig");

const common = @import("common.zig");
const SearchResults = common.SearchResults;
const Change = common.Change;

const Deadline = @import("utils/Deadline.zig");

const Segment = @import("Segment.zig");
const Segments = std.DoublyLinkedList(Segment);

const Oplog = @import("Oplog.zig");

const filefmt = @import("filefmt.zig");

const Self = @This();

const Options = struct {
    min_segment_size: usize = 1000,
};

options: Options,

dir: std.fs.Dir,
allocator: std.mem.Allocator,
stage: InMemoryIndex,
segments: Segments,
write_lock: std.Thread.RwLock = .{},

scheduler: zul.Scheduler(Task, *Self),
last_cleanup_at: i64 = 0,
cleanup_interval: i64 = 1000,

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
        .segments = .{},
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
    while (self.segments.popFirst()) |node| {
        self.destroySegment(node);
    }
}

fn getMaxCommitId(self: *Self) u64 {
    var max_commit_id: u64 = 0;
    var it = self.segments.first;
    while (it) |node| : (it = node.next) {
        if (node.data.max_commit_id > max_commit_id) {
            max_commit_id = node.data.max_commit_id;
        }
    }
    return max_commit_id;
}

pub fn open(self: *Self) !void {
    try self.scheduler.start(self);
    try self.read();
    try self.oplog.open(self.getMaxCommitId());
}

fn createSegment(self: *Self) !*Segments.Node {
    const node = try self.allocator.create(Segments.Node);
    node.data = Segment.init(self.allocator);
    return node;
}

fn destroySegment(self: *Self, node: *Segments.Node) void {
    node.data.deinit();
    self.allocator.destroy(node);
}

const index_file_name = "index.dat";

fn write(self: *Self) !void {
    var file = try self.dir.atomicFile(index_file_name, .{});
    defer file.deinit();

    var segments = std.ArrayList(Segment.Version).init(self.allocator);
    defer segments.deinit();

    try segments.ensureTotalCapacity(self.segments.len);

    var it = self.segments.first;
    while (it) |node| : (it = node.next) {
        try segments.append(node.data.version);
    }

    try filefmt.writeIndexFile(file.file.writer(), segments);

    try file.finish();
}

fn read(self: *Self) !void {
    if (self.segments.len > 0) {
        return error.AlreadyOpened;
    }

    var file = self.dir.openFile(index_file_name, .{}) catch |err| {
        if (err == error.FileNotFound) {
            return;
        }
        return err;
    };
    defer file.close();

    var segments = std.ArrayList(Segment.Version).init(self.allocator);
    defer segments.deinit();

    try filefmt.readIndexFile(file.reader(), &segments);

    for (segments.items) |v| {
        const node = try self.createSegment();
        try node.data.open(self.dir, v);
        self.segments.append(node);
    }
}

fn cleanup(self: *Self) !void {
    log.info("running cleanup", .{});

    var max_commit_id: ?u64 = null;

    if (self.stage.freezeFirstSegment()) |segment| {
        var commited = false;
        const node = try self.createSegment();
        defer {
            if (!commited) self.destroySegment(node);
        }

        try node.data.convert(self.dir, segment);

        self.write_lock.lock();
        defer self.write_lock.unlock();

        try self.write();

        self.stage.removeSegment(segment);
        self.segments.append(node);
        max_commit_id = node.data.max_commit_id;
        commited = true;
    }

    if (max_commit_id) |commit_id| {
        self.oplog.truncate(commit_id) catch |err| {
            log.err("failed to truncate oplog: {}", .{err});
        };
    }
}

pub fn update(self: *Self, changes: []const Change) !void {
    const commit_id = try self.oplog.write(changes);
    try self.stage.update(changes, commit_id);
    try self.scheduler.scheduleIn(.{ .cleanup = {} }, 0);
}

pub fn search(self: *Self, hashes: []const u32, results: *SearchResults, deadline: Deadline) !void {
    self.write_lock.lockShared();
    defer self.write_lock.unlockShared();

    const sorted_hashes = try self.allocator.dupe(u32, hashes);
    defer self.allocator.free(sorted_hashes);
    std.sort.pdq(u32, sorted_hashes, {}, std.sort.asc(u32));

    var it = self.segments.first;
    while (it) |node| : (it = node.next) {
        if (deadline.isExpired()) {
            return error.Timeout;
        }
        try node.data.search(sorted_hashes, results);
    }

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
