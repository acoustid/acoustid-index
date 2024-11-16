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
    min_segment_size: usize = 1000,
};

options: Options,

allocator: std.mem.Allocator,
dir: std.fs.Dir,
scheduler: *Scheduler,

stage: InMemoryIndex,

index: FileIndex,
index_dir: std.fs.Dir,

oplog: Oplog,
oplog_dir: std.fs.Dir,

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
}

pub fn open(self: *Self) !void {
    try self.index.open();
    try self.oplog.open(self.index.getMaxCommitId(), &self.stage);
}

pub fn update(self: *Self, changes: []const Change) !void {
    try self.oplog.write(changes, &self.stage);
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
