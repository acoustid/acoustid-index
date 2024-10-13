const std = @import("std");
const fs = std.fs;
const log = std.log.scoped(.index);

const zul = @import("zul");

const InMemoryIndex = @import("InMemoryIndex.zig");

const common = @import("common.zig");
const SearchResults = common.SearchResults;
const Change = common.Change;

const Deadline = @import("utils/Deadline.zig");

const Segment = @import("Segment.zig");
const Segments = std.DoublyLinkedList(Segment);

const Self = @This();

dir: fs.Dir,
allocator: std.mem.Allocator,
stage: InMemoryIndex,
segments: Segments,

scheduler: zul.Scheduler(Task, *Self),
last_cleanup_at: i64 = 0,
cleanup_interval: i64 = 1000,

const min_segment_size = 1_000_000;

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

pub fn init(allocator: std.mem.Allocator, dir: fs.Dir) Self {
    var self = Self{
        .dir = dir,
        .allocator = allocator,
        .stage = InMemoryIndex.init(allocator),
        .segments = .{},
        .scheduler = zul.Scheduler(Task, *Self).init(allocator),
    };
    self.stage.auto_cleanup = true;
    return self;
}

pub fn start(self: *Self) !void {
    try self.scheduler.start(self);
}

pub fn deinit(self: *Self) void {
    self.stage.deinit();
    while (self.segments.popFirst()) |node| {
        node.data.deinit();
        self.allocator.destroy(node);
    }
    self.scheduler.deinit();
}

fn cleanup(self: *Self) !void {
    log.info("running cleanup", .{});

    // try self.stage.cleanup();

    if (false) {
        const segment = self.stage.freezeFirstSegment(min_segment_size);
        if (segment) |s| {
            const name = try std.fmt.allocPrint(self.allocator, "segment_{}_{}.dat", .{ s.version - s.merged, s.version });
            defer self.allocator.free(name);
            log.info("writing segment {s} to disk", .{name});
            var file = try self.dir.createFile(name, .{});
            defer file.close();
            try s.write(file.writer());
        }
    }
}

pub fn update(self: *Self, changes: []const Change) !void {
    try self.stage.update(changes);
    try self.scheduler.scheduleIn(.{ .cleanup = {} }, 0);
}

pub fn search(self: *Self, hashes: []const u32, results: *SearchResults, deadline: Deadline) !void {
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

    var index = Self.init(std.testing.allocator, tmpDir.dir);
    defer index.deinit();

    try index.start();

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