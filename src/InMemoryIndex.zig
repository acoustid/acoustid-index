const std = @import("std");
const log = std.log;

const Deadline = @import("utils/Deadline.zig");

const assert = std.debug.assert;

const common = @import("common.zig");
const Item = common.Item;
const SearchResults = common.SearchResults;
const Change = common.Change;

const Segment = @import("InMemorySegment.zig");
const Segments = std.DoublyLinkedList(Segment);

allocator: std.mem.Allocator,
write_lock: std.Thread.RwLock,
merge_lock: std.Thread.Mutex,
segments: Segments,
max_segments: usize = 16,

const Self = @This();

pub fn init(allocator: std.mem.Allocator) Self {
    return .{
        .allocator = allocator,
        .write_lock = .{},
        .merge_lock = .{},
        .segments = .{},
    };
}

pub fn deinit(self: *Self) void {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    while (self.segments.popFirst()) |node| {
        self.destroyNode(node);
    }
}

fn destroyNode(self: *Self, node: *Segments.Node) void {
    node.data.deinit();
    self.allocator.destroy(node);
}

pub fn update(self: *Self, changes: []const Change) !void {
    var committed = false;

    const node = try self.allocator.create(Segments.Node);
    defer {
        if (!committed) self.allocator.destroy(node);
    }

    node.data = Segment.init(self.allocator);
    defer {
        if (!committed) {
            node.data.deinit();
        }
    }

    var num_items: usize = 0;
    for (changes) |change| {
        switch (change) {
            .insert => |op| {
                num_items += op.hashes.len;
            },
            .delete => {},
        }
    }
    try node.data.items.ensureTotalCapacity(num_items);

    var i = changes.len;
    while (i > 0) {
        i -= 1;
        const change = changes[i];
        switch (change) {
            .insert => |op| {
                const result = try node.data.docs.getOrPut(op.id);
                if (!result.found_existing) {
                    result.value_ptr.* = true;
                    var items = try node.data.items.addManyAsSlice(op.hashes.len);
                    for (op.hashes, 0..) |hash, j| {
                        items[j] = .{ .hash = hash, .id = op.id };
                    }
                }
            },
            .delete => |op| {
                const result = try node.data.docs.getOrPut(op.id);
                if (!result.found_existing) {
                    result.value_ptr.* = false;
                }
            },
        }
    }

    node.data.ensureSorted();

    var needs_merging = false;

    self.write_lock.lock();
    self.segments.append(node);
    if (node.prev) |prev| {
        node.data.version = prev.data.version + 1;
    } else {
        node.data.version = 1;
    }
    if (self.segments.len > self.max_segments) {
        needs_merging = true;
    }
    self.checkSegments();
    committed = true;
    self.write_lock.unlock();

    if (needs_merging) {
        self.mergeSegments() catch |err| {
            std.debug.print("mergeSegments failed: {}\n", .{err});
        };
    }
}

fn hasNewerVersion(self: *Self, id: u32, version: u32) bool {
    var it = self.segments.last;
    while (it) |node| : (it = node.prev) {
        if (node.data.version > version) {
            if (node.data.docs.contains(id)) {
                return true;
            }
        } else {
            break;
        }
    }
    return false;
}

const Merge = struct {
    first: *Segments.Node,
    last: *Segments.Node,
    replacement: *Segments.Node,
};

fn prepareMerge(self: *Self) !?Merge {
    self.write_lock.lockShared();
    defer self.write_lock.unlockShared();

    var total_size: usize = 0;
    var num_segments: usize = 0;
    var segments_iter = self.segments.first;
    while (segments_iter) |node| : (segments_iter = node.next) {
        if (node.data.frozen) {
            continue;
        }
        total_size += node.data.items.items.len;
        num_segments += 1;
    }
    const avg_size = total_size / num_segments;

    var bestNode: ?*Segments.Node = null;
    var bestScore: usize = std.math.maxInt(usize);
    segments_iter = self.segments.first;
    while (segments_iter) |node| : (segments_iter = node.next) {
        if (node.data.frozen) {
            continue;
        }
        if (node.next) |nextNode| {
            const size = node.data.items.items.len + nextNode.data.items.items.len;
            const score = if (size > avg_size) size - avg_size else avg_size - size;
            if (score < bestScore) {
                bestNode = node;
                bestScore = score;
            }
        }
    }

    if (bestNode == null or bestNode.?.next == null) {
        return null;
    }

    const node1 = bestNode.?;
    const node2 = bestNode.?.next.?;

    const segment1 = node1.data;
    const segment2 = node2.data;
    const segments = [2]Segment{ segment1, segment2 };

    var committed = false;

    const node = try self.allocator.create(Segments.Node);
    defer {
        if (!committed) self.allocator.destroy(node);
    }

    const merge = Merge{ .first = node1, .last = node2, .replacement = node };

    node.data = Segment.init(self.allocator);
    defer {
        if (!committed) {
            node.data.deinit();
        }
    }
    node.data.version = segment2.version;
    node.data.merged = segment1.merged + segment2.merged + 1;

    log.debug("Merging in-memory segments {}:{} and {}:{}", .{ segment1.version, segment1.merged, segment2.version, segment2.merged });

    var total_docs: usize = 0;
    var total_items: usize = 0;
    for (segments) |segment| {
        total_docs += segment.docs.count();
        total_items += segment.items.items.len;
    }

    try node.data.docs.ensureUnusedCapacity(@truncate(total_docs));
    try node.data.items.ensureTotalCapacity(total_items);

    {
        var skip_docs = std.AutoHashMap(u32, void).init(self.allocator);
        defer skip_docs.deinit();

        try skip_docs.ensureTotalCapacity(@truncate(total_docs / 10));

        for (segments) |segment| {
            skip_docs.clearRetainingCapacity();

            var docs_iter = segment.docs.iterator();
            while (docs_iter.next()) |entry| {
                const id = entry.key_ptr.*;
                const status = entry.value_ptr.*;
                if (!self.hasNewerVersion(id, segment.version)) {
                    try node.data.docs.put(id, status);
                } else {
                    try skip_docs.put(id, {});
                }
            }

            for (segment.items.items) |item| {
                if (!skip_docs.contains(item.id)) {
                    try node.data.items.append(item);
                }
            }
        }
    }

    node.data.ensureSorted();

    committed = true;
    return merge;
}

fn checkSegments(self: *Self) void {
    if (std.debug.runtime_safety) {
        var iter = self.segments.first;
        while (iter) |node| : (iter = node.next) {
            if (node.prev) |prev| {
                assert(node.data.version == 1 + node.data.merged + prev.data.version);
            } else {
                assert(node.data.version == 1 + node.data.merged);
            }
        }
    }
}

fn commitMerge(self: *Self, merge: Merge) void {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    log.debug("Adding in-memory segment {}:{}", .{ merge.replacement.data.version, merge.replacement.data.merged });
    self.segments.insertAfter(merge.last, merge.replacement);

    var iter: ?*Segments.Node = merge.first;
    while (iter) |node| {
        iter = node.next;
        log.debug("Removing in-memory segment {}:{}", .{ node.data.version, node.data.merged });
        self.segments.remove(node);
        self.destroyNode(node);
        if (node == merge.last) break;
    }

    self.checkSegments();
}

fn mergeSegments(self: *Self) !void {
    self.merge_lock.lock();
    defer self.merge_lock.unlock();

    const maybeMerge = try self.prepareMerge();
    if (maybeMerge) |merge| {
        self.commitMerge(merge);
    }
}

pub fn freezeFirstSegment(self: *Self) ?*Segment {
    self.merge_lock.lock();
    defer self.merge_lock.unlock();

    self.write_lock.lockShared();
    defer self.write_lock.unlockShared();

    const first = self.segments.first;
    if (first) |node| {
        if (node.next != null) { // only continue if there is more than one segment
            const segment = &node.data;
            segment.frozen = true;
            return segment;
        }
    }
    return null;
}

pub fn search(self: *Self, hashes: []const u32, results: *SearchResults, deadline: Deadline) !void {
    self.write_lock.lockShared();
    defer self.write_lock.unlockShared();

    var previousSegmentVersion: u32 = 0;
    var segmentIter = self.segments.first;
    while (segmentIter) |node| : (segmentIter = node.next) {
        const segment = &node.data;

        if (deadline.isExpired()) {
            return error.Timeout;
        }

        assert(segment.version > previousSegmentVersion);
        previousSegmentVersion = segment.version;

        try segment.search(hashes, results);

        // Remove results for docs that have been updated/deleted in the current segment.
        // We can do it here, because we know previously processed segments always have
        // lower version numbers.
        var results_iter = results.results.iterator();
        while (results_iter.next()) |result| {
            const version = result.value_ptr.version;
            if (version < segment.version) {
                if (segment.docs.contains(result.key_ptr.*)) {
                    result.value_ptr.score = 0;
                    result.value_ptr.version = segment.version;
                }
            }
        }
    }
}

test "insert and search" {
    var index = Self.init(std.testing.allocator);
    defer index.deinit();

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

test "insert, partial update and search" {
    var index = Self.init(std.testing.allocator);
    defer index.deinit();

    try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }});

    try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 1, 2, 4 },
    } }});

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
    var index = Self.init(std.testing.allocator);
    defer index.deinit();

    try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }});

    try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 100, 200, 300 },
    } }});

    var results = SearchResults.init(std.testing.allocator);
    defer results.deinit();

    try index.search(&[_]u32{ 1, 2, 3 }, &results, .{});

    const result = results.get(1);
    try std.testing.expect(result == null or result.?.score == 0);
}

test "insert, full update (multiple times) and search" {
    var index = Self.init(std.testing.allocator);
    defer index.deinit();

    var i: u32 = 1000;
    while (i > 0) : (i -= 1) {
        try index.update(&[_]Change{.{ .insert = .{
            .id = i % 10,
            .hashes = &[_]u32{ i * 1000 + 1, i * 1000 + 2, i * 1000 + 3 },
        } }});
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
    var index = Self.init(std.testing.allocator);
    defer index.deinit();

    try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }});

    try index.update(&[_]Change{.{ .delete = .{
        .id = 1,
    } }});

    var results = SearchResults.init(std.testing.allocator);
    defer results.deinit();

    try index.search(&[_]u32{ 1, 2, 3 }, &results, .{});

    const result = results.get(1);
    try std.testing.expect(result == null or result.?.score == 0);
}

test "freeze segment" {
    var index = Self.init(std.testing.allocator);
    defer index.deinit();

    try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }});

    const segment1 = index.freezeFirstSegment();
    try std.testing.expect(segment1 == null);

    for (0..100) |_| {
        try index.update(&[_]Change{.{ .insert = .{
            .id = 1,
            .hashes = &[_]u32{ 1, 2, 3 },
        } }});
    }

    const segment2 = index.freezeFirstSegment();
    try std.testing.expect(segment2 != null);
    try std.testing.expect(segment2.?.frozen);

    const segment3 = index.freezeFirstSegment();
    try std.testing.expect(segment3 == segment2);
}
