const std = @import("std");
const assert = std.debug.assert;
const log = std.log;

const Deadline = @import("utils/Deadline.zig");

const common = @import("common.zig");
const Item = common.Item;
const SearchResults = common.SearchResults;
const Change = common.Change;

const InMemorySegment = @import("InMemorySegment.zig");
const InMemorySegmentList = InMemorySegment.List;

const Options = struct {
    max_segment_size: usize = 1_000_000,
};

options: Options,
allocator: std.mem.Allocator,
write_lock: std.Thread.RwLock,
merge_lock: std.Thread.Mutex,
segments: InMemorySegmentList,

const Self = @This();

pub fn init(allocator: std.mem.Allocator, options: Options) Self {
    return .{
        .options = options,
        .allocator = allocator,
        .write_lock = .{},
        .merge_lock = .{},
        .segments = InMemorySegmentList.init(allocator),
    };
}

pub fn deinit(self: *Self) void {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    self.segments.deinit();
}

pub const PendingUpdate = struct {
    node: *InMemorySegmentList.List.Node,
    finished: bool = false,
};

// Cancels the update, does nothing if it has already been cancelled or committted.
pub fn cancelUpdate(self: *Self, txn: *PendingUpdate) void {
    if (txn.finished) return;

    self.segments.destroySegment(txn.node);

    txn.finished = true;
    self.write_lock.unlock();
}

// Prepares update for later commit, will block until previous update has been committed.
pub fn prepareUpdate(self: *Self, changes: []const Change) !PendingUpdate {
    try self.maybeMergeSegments();

    const node = try self.segments.createSegment();
    errdefer self.segments.destroySegment(node);

    try node.data.build(changes);

    self.write_lock.lock();
    return PendingUpdate{ .node = node };
}

// Commits the update, does nothing if it has already been cancelled or committted.
pub fn commitUpdate(self: *Self, txn: *PendingUpdate, commit_id: u64) void {
    if (txn.finished) return;

    self.segments.segments.append(txn.node);

    txn.node.data.max_commit_id = commit_id;
    if (txn.node.prev) |prev| {
        txn.node.data.id = prev.data.id.next();
    } else {
        txn.node.data.id = common.SegmentID.first();
    }

    txn.finished = true;
    self.write_lock.unlock();
}

pub fn update(self: *Self, changes: []const Change, commit_id: u64) !void {
    var txn = try self.prepareUpdate(changes);
    self.commitUpdate(&txn, commit_id);
}

fn checkSegments(self: *Self) void {
    var iter = self.segments.segments.first;
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

fn prepareMerge(self: *Self) !?InMemorySegmentList.PreparedMerge {
    self.write_lock.lockShared();
    defer self.write_lock.unlockShared();

    const merge = try self.segments.prepareMerge(.{ .max_segment_size = self.options.max_segment_size }) orelse return null;
    errdefer self.segments.destroySegment(merge.target);

    try merge.target.data.merge(&merge.sources.node1.data, &merge.sources.node2.data, &self.segments);

    return merge;
}

fn finnishMerge(self: *Self, merge: InMemorySegmentList.PreparedMerge) void {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    self.segments.applyMerge(merge);
    self.segments.destroyMergedSegments(merge);

    self.checkSegments();
}

// Perform partial compaction on the in-memory segments.
fn maybeMergeSegments(self: *Self) !void {
    self.merge_lock.lock();
    defer self.merge_lock.unlock();

    if (try self.prepareMerge()) |merge| {
        self.finnishMerge(merge);
    }
}

// Freezes the oldest segment, if the segment is already at its max size.
// This is called periodically by the cleanup process of the main index.
// Frozen segments are then persisted to disk and removed from the in-memory index.
pub fn maybeFreezeOldestSegment(self: *Self) ?*InMemorySegment {
    self.merge_lock.lock();
    defer self.merge_lock.unlock();

    self.write_lock.lockShared();
    defer self.write_lock.unlockShared();

    const first = self.segments.segments.first;
    if (first) |node| {
        if (node.next != null) { // only continue if there is more than one segment
            const segment = &node.data;
            if (segment.frozen) {
                return segment;
            }
            if (segment.items.items.len >= self.options.max_segment_size) {
                segment.frozen = true;
                return segment;
            }
        }
    }
    return null;
}

// Removes previously frozen segment that is no longer needed.
// This is called from the cleanup process of the main index, when the
// segment has already been persisted to disk.
pub fn removeFrozenSegment(self: *Self, segment: *InMemorySegment) void {
    self.merge_lock.lock();
    defer self.merge_lock.unlock();

    self.write_lock.lock();
    defer self.write_lock.unlock();

    var it = self.segments.segments.first;
    while (it) |node| : (it = node.next) {
        if (&node.data == segment) {
            if (node.data.frozen) {
                self.segments.removeAndDestroy(node);
                return;
            }
        }
    }
}

pub fn search(self: *Self, hashes: []const u32, results: *SearchResults, deadline: Deadline) !void {
    self.write_lock.lockShared();
    defer self.write_lock.unlockShared();

    try self.segments.search(hashes, results, deadline);
}

test "insert and search" {
    var index = Self.init(std.testing.allocator, .{});
    defer index.deinit();

    try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }}, 1);

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

test "insert, partial update and search" {
    var index = Self.init(std.testing.allocator, .{});
    defer index.deinit();

    try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }}, 1);

    try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 1, 2, 4 },
    } }}, 2);

    var results = SearchResults.init(std.testing.allocator);
    defer results.deinit();

    try index.search(&[_]u32{ 1, 2, 3 }, &results, .{});

    try std.testing.expectEqual(1, results.count());

    const result = results.get(1);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(1, result.?.id);
    try std.testing.expectEqual(2, result.?.score);
    try std.testing.expect(result.?.version != 0);
}

test "insert, full update and search" {
    var index = Self.init(std.testing.allocator, .{});
    defer index.deinit();

    try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }}, 1);

    try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 100, 200, 300 },
    } }}, 2);

    var results = SearchResults.init(std.testing.allocator);
    defer results.deinit();

    try index.search(&[_]u32{ 1, 2, 3 }, &results, .{});

    const result = results.get(1);
    try std.testing.expect(result == null or result.?.score == 0);
}

test "insert, full update (multiple times) and search" {
    var index = Self.init(std.testing.allocator, .{});
    defer index.deinit();

    var commit_id: u64 = 1;

    var i: u32 = 1000;
    while (i > 0) : (i -= 1) {
        try index.update(&[_]Change{.{ .insert = .{
            .id = i % 10,
            .hashes = &[_]u32{ i * 1000 + 1, i * 1000 + 2, i * 1000 + 3 },
        } }}, commit_id);
        commit_id += 1;
    }
    i += 1;

    var results = SearchResults.init(std.testing.allocator);
    defer results.deinit();

    try index.search(&[_]u32{ i * 1000 + 1, i * 1000 + 2, i * 1000 + 3 }, &results, .{});

    try std.testing.expectEqual(1, results.count());

    const result = results.get(i % 10);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(1, result.?.id);
    try std.testing.expectEqual(3, result.?.score);
    try std.testing.expect(result.?.version != 0);
}

test "insert, delete and search" {
    var index = Self.init(std.testing.allocator, .{});
    defer index.deinit();

    try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }}, 1);

    try index.update(&[_]Change{.{ .delete = .{
        .id = 1,
    } }}, 2);

    var results = SearchResults.init(std.testing.allocator);
    defer results.deinit();

    try index.search(&[_]u32{ 1, 2, 3 }, &results, .{});

    const result = results.get(1);
    try std.testing.expect(result == null or result.?.score == 0);
}

test "freeze segment" {
    var index = Self.init(std.testing.allocator, .{ .max_segment_size = 100 });
    defer index.deinit();

    try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }}, 1);

    const segment1 = index.maybeFreezeOldestSegment();
    try std.testing.expect(segment1 == null);

    var commit_id: u64 = 2;
    for (1..100) |i| {
        try index.update(&[_]Change{.{ .insert = .{
            .id = @intCast(i),
            .hashes = &[_]u32{ 1, 2, 3 },
        } }}, commit_id);
        commit_id += 1;
    }

    const segment2 = index.maybeFreezeOldestSegment();
    try std.testing.expect(segment2 != null);
    try std.testing.expect(segment2.?.frozen);

    const segment3 = index.maybeFreezeOldestSegment();
    try std.testing.expect(segment3 == segment2);
}
