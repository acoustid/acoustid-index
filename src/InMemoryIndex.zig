const std = @import("std");
const log = std.log;

const Deadline = @import("utils/Deadline.zig");

const assert = std.debug.assert;

const common = @import("common.zig");
const Item = common.Item;
const SearchResultHashMap = common.SearchResultHashMap;

const Segment = @import("InMemorySegment.zig");
const Segments = std.DoublyLinkedList(Segment);

allocator: std.mem.Allocator,
writeLock: std.Thread.RwLock,
mergeLock: std.Thread.Mutex,
segments: Segments,
maxSegments: usize = 16,

const Self = @This();

pub const Insert = struct {
    docId: u32,
    hashes: []const u32,
};

pub const Delete = struct {
    docId: u32,
};

pub const Change = union(enum) {
    insert: Insert,
    delete: Delete,
};

pub fn init(allocator: std.mem.Allocator) Self {
    return .{
        .allocator = allocator,
        .writeLock = .{},
        .mergeLock = .{},
        .segments = .{},
    };
}

pub fn deinit(self: *Self) void {
    self.writeLock.lock();
    defer self.writeLock.unlock();

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

    var numItems: usize = 0;
    for (changes) |change| {
        switch (change) {
            .insert => |op| {
                numItems += op.hashes.len;
            },
            .delete => {},
        }
    }
    try node.data.items.ensureTotalCapacity(numItems);

    var i = changes.len;
    while (i > 0) {
        i -= 1;
        const change = changes[i];
        switch (change) {
            .insert => |op| {
                const docId = op.docId;
                const result = try node.data.docs.getOrPut(docId);
                if (!result.found_existing) {
                    result.value_ptr.* = true;
                    var items = try node.data.items.addManyAsSlice(op.hashes.len);
                    for (op.hashes, 0..) |hash, j| {
                        items[j] = .{ .hash = hash, .docId = docId };
                    }
                }
            },
            .delete => |op| {
                const docId = op.docId;
                const result = try node.data.docs.getOrPut(docId);
                if (!result.found_existing) {
                    result.value_ptr.* = false;
                }
            },
        }
    }

    node.data.ensureSorted();

    var needsMerging = false;

    self.writeLock.lock();
    self.segments.append(node);
    if (node.prev) |prev| {
        node.data.version = prev.data.version + 1;
    } else {
        node.data.version = 1;
    }
    if (self.segments.len > self.maxSegments) {
        needsMerging = true;
    }
    self.checkSegments();
    committed = true;
    self.writeLock.unlock();

    if (needsMerging) {
        self.mergeSegments() catch |err| {
            std.debug.print("mergeSegments failed: {}\n", .{err});
        };
    }
}

fn hasNewerVersion(self: *Self, docId: u32, version: u32) bool {
    var it = self.segments.last;
    while (it) |node| : (it = node.prev) {
        if (node.data.version > version) {
            if (node.data.docs.contains(docId)) {
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
    self.writeLock.lockShared();
    defer self.writeLock.unlockShared();

    var totalSize: usize = 0;
    {
        var segmentIter = self.segments.first;
        while (segmentIter) |node| : (segmentIter = node.next) {
            totalSize += node.data.items.items.len;
        }
    }
    const avgSize = totalSize / self.segments.len;

    var bestNode: ?*Segments.Node = null;
    var bestScore: usize = std.math.maxInt(usize);
    var segmentIter = self.segments.first;
    while (segmentIter) |node| : (segmentIter = node.next) {
        if (node.next) |nextNode| {
            const size = node.data.items.items.len + nextNode.data.items.items.len;
            const score = if (size > avgSize) size - avgSize else avgSize - size;
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

    var totalDocs: usize = 0;
    var totalItems: usize = 0;
    for (segments) |segment| {
        totalDocs += segment.docs.count();
        totalItems += segment.items.items.len;
    }

    try node.data.docs.ensureUnusedCapacity(@truncate(totalDocs));
    try node.data.items.ensureTotalCapacity(totalItems);

    {
        var skipDocs = std.AutoHashMap(u32, void).init(self.allocator);
        defer skipDocs.deinit();

        try skipDocs.ensureTotalCapacity(@truncate(totalDocs / 10));

        for (segments) |segment| {
            skipDocs.clearRetainingCapacity();

            var docsIter = segment.docs.iterator();
            while (docsIter.next()) |entry| {
                const docId = entry.key_ptr.*;
                const status = entry.value_ptr.*;
                if (!self.hasNewerVersion(docId, segment.version)) {
                    try node.data.docs.put(docId, status);
                } else {
                    try skipDocs.put(docId, {});
                }
            }

            for (segment.items.items) |item| {
                if (!skipDocs.contains(item.docId)) {
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
    self.writeLock.lock();
    defer self.writeLock.unlock();

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
    self.mergeLock.lock();
    defer self.mergeLock.unlock();

    const maybeMerge = try self.prepareMerge();
    if (maybeMerge) |merge| {
        self.commitMerge(merge);
    }
}

pub fn search(self: *Self, hashes: []const u32, results: *SearchResultHashMap, deadline: Deadline) !void {
    self.writeLock.lockShared();
    defer self.writeLock.unlockShared();

    var previousSegmentVersion: u32 = 0;
    var segmentIter = self.segments.first;
    while (segmentIter) |node| : (segmentIter = node.next) {
        const segment = &node.data;
        const items = segment.items.items;

        if (deadline.isExpired()) {
            return error.Timeout;
        }

        assert(segment.version > previousSegmentVersion);
        previousSegmentVersion = segment.version;

        var previousHash: u32 = 0;
        var previousHashStartedAt: usize = 0;
        var previousHashEndedAt: usize = 0;
        for (hashes) |hash| {
            var i = previousHashStartedAt;
            if (hash > previousHash) {
                const offset = std.sort.lowerBound(Item, Item{ .hash = hash, .docId = 0 }, items[previousHashEndedAt..], {}, Item.cmp);
                i = previousHashEndedAt + offset;
                previousHash = hash;
                previousHashStartedAt = i;
            } else {
                assert(hash == previousHash);
            }
            while (i < items.len and items[i].hash == hash) : (i += 1) {
                const docId = items[i].docId;
                const r = try results.getOrPut(docId);
                if (!r.found_existing or r.value_ptr.version < segment.version) {
                    r.value_ptr.docId = docId;
                    r.value_ptr.score = 1;
                    r.value_ptr.version = segment.version;
                } else if (r.value_ptr.version == segment.version) {
                    r.value_ptr.score += 1;
                }
                previousHashEndedAt = i;
            }
        }

        // Remove results for docs that have been updated/deleted in the current segment.
        // We can do it here, because we know previously processed segments always have
        // lower version numbers.
        var resultIter = results.iterator();
        while (resultIter.next()) |result| {
            const version = result.value_ptr.version;
            if (version < segment.version) {
                const docId = result.key_ptr.*;
                if (segment.docs.contains(docId)) {
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
        .docId = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }});

    var results = SearchResultHashMap.init(std.testing.allocator);
    defer results.deinit();

    try index.search(&[_]u32{ 1, 2, 3 }, &results, .{});

    try std.testing.expectEqual(1, results.count());

    const result = results.get(1);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(1, result.?.docId);
    try std.testing.expectEqual(3, result.?.score);
    try std.testing.expect(result.?.version != 0);
}

test "insert, partial update and search" {
    var index = Self.init(std.testing.allocator);
    defer index.deinit();

    try index.update(&[_]Change{.{ .insert = .{
        .docId = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }});

    try index.update(&[_]Change{.{ .insert = .{
        .docId = 1,
        .hashes = &[_]u32{ 1, 2, 4 },
    } }});

    var results = SearchResultHashMap.init(std.testing.allocator);
    defer results.deinit();

    try index.search(&[_]u32{ 1, 2, 3 }, &results, .{});

    try std.testing.expectEqual(1, results.count());

    const result = results.get(1);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(1, result.?.docId);
    try std.testing.expectEqual(2, result.?.score);
    try std.testing.expect(result.?.version != 0);
}

test "insert, full update and search" {
    var index = Self.init(std.testing.allocator);
    defer index.deinit();

    try index.update(&[_]Change{.{ .insert = .{
        .docId = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }});

    try index.update(&[_]Change{.{ .insert = .{
        .docId = 1,
        .hashes = &[_]u32{ 100, 200, 300 },
    } }});

    var results = SearchResultHashMap.init(std.testing.allocator);
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
            .docId = i % 10,
            .hashes = &[_]u32{ i * 1000 + 1, i * 1000 + 2, i * 1000 + 3 },
        } }});
    }
    i += 1;

    var results = SearchResultHashMap.init(std.testing.allocator);
    defer results.deinit();

    try index.search(&[_]u32{ i * 1000 + 1, i * 1000 + 2, i * 1000 + 3 }, &results, .{});

    try std.testing.expectEqual(1, results.count());

    const result = results.get(i % 10);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(1, result.?.docId);
    try std.testing.expectEqual(3, result.?.score);
    try std.testing.expect(result.?.version != 0);
}

test "insert, delete and search" {
    var index = Self.init(std.testing.allocator);
    defer index.deinit();

    try index.update(&[_]Change{.{ .insert = .{
        .docId = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }});

    try index.update(&[_]Change{.{ .delete = .{
        .docId = 1,
    } }});

    var results = SearchResultHashMap.init(std.testing.allocator);
    defer results.deinit();

    try index.search(&[_]u32{ 1, 2, 3 }, &results, .{});

    const result = results.get(1);
    try std.testing.expect(result == null or result.?.score == 0);
}
