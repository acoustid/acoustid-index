const std = @import("std");
const log = std.log;

const Deadline = @import("utils/Deadline.zig");

const assert = std.debug.assert;

const common = @import("common.zig");
const Item = common.Item;
const SearchResultHashMap = common.SearchResultHashMap;

allocator: std.mem.Allocator,
writeLock: std.Thread.RwLock,
mergeLock: std.Thread.Mutex,
blocks: Blocks,
maxBlocks: usize = 16,

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

const Block = struct {
    version: u32,
    docs: std.AutoHashMap(u32, bool),
    items: std.ArrayList(Item),
    frozen: bool = false,
    merged: u32 = 0,
};

const Blocks = std.DoublyLinkedList(Block);

pub fn init(allocator: std.mem.Allocator) Self {
    return .{
        .allocator = allocator,
        .writeLock = .{},
        .mergeLock = .{},
        .blocks = .{},
    };
}

pub fn deinit(self: *Self) void {
    self.writeLock.lock();
    defer self.writeLock.unlock();

    while (self.blocks.popFirst()) |node| {
        self.destroyNode(node);
    }
}

fn destroyNode(self: *Self, node: *Blocks.Node) void {
    node.data.docs.deinit();
    node.data.items.deinit();
    self.allocator.destroy(node);
}

pub fn update(self: *Self, changes: []const Change) !void {
    var committed = false;

    const node = try self.allocator.create(Blocks.Node);
    defer {
        if (!committed) self.allocator.destroy(node);
    }

    node.data = Block{
        .version = 1,
        .docs = std.AutoHashMap(u32, bool).init(self.allocator),
        .items = std.ArrayList(Item).init(self.allocator),
    };
    defer {
        if (!committed) {
            node.data.items.deinit();
            node.data.docs.deinit();
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

    std.sort.pdq(Item, node.data.items.items, {}, Item.cmp);

    var needsMerging = false;

    self.writeLock.lock();
    self.blocks.append(node);
    if (node.prev) |prev| {
        node.data.version = prev.data.version + 1;
    }
    if (self.blocks.len > self.maxBlocks) {
        needsMerging = true;
    }
    self.checkBlocks();
    committed = true;
    self.writeLock.unlock();

    if (needsMerging) {
        self.mergeBlocks() catch |err| {
            std.debug.print("mergeBlocks failed: {}\n", .{err});
        };
    }
}

fn hasNewerVersion(self: *Self, docId: u32, version: u32) bool {
    var it = self.blocks.last;
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
    first: *Blocks.Node,
    last: *Blocks.Node,
    replacement: *Blocks.Node,
};

fn prepareMerge(self: *Self) !?Merge {
    self.writeLock.lockShared();
    defer self.writeLock.unlockShared();

    var totalSize: usize = 0;
    {
        var blockIter = self.blocks.first;
        while (blockIter) |node| : (blockIter = node.next) {
            totalSize += node.data.items.items.len;
        }
    }
    const avgSize = totalSize / self.blocks.len;

    var bestNode: ?*Blocks.Node = null;
    var bestScore: usize = std.math.maxInt(usize);
    var blockIter = self.blocks.first;
    while (blockIter) |node| : (blockIter = node.next) {
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

    const block1 = node1.data;
    const block2 = node2.data;
    const blocks = [2]Block{ block1, block2 };

    var committed = false;

    const node = try self.allocator.create(Blocks.Node);
    defer {
        if (!committed) self.allocator.destroy(node);
    }

    const merge = Merge{ .first = node1, .last = node2, .replacement = node };

    node.data = Block{
        .version = block2.version,
        .docs = std.AutoHashMap(u32, bool).init(self.allocator),
        .items = std.ArrayList(Item).init(self.allocator),
        .merged = block1.merged + block2.merged + 1,
    };
    defer {
        if (!committed) {
            node.data.items.deinit();
            node.data.docs.deinit();
        }
    }

    log.debug("Merging in-memory blocks {}:{} and {}:{}", .{ block1.version, block1.merged, block2.version, block2.merged });

    var totalDocs: usize = 0;
    var totalItems: usize = 0;
    for (blocks) |block| {
        totalDocs += block.docs.count();
        totalItems += block.items.items.len;
    }

    try node.data.docs.ensureUnusedCapacity(@truncate(totalDocs));
    try node.data.items.ensureTotalCapacity(totalItems);

    {
        var skipDocs = std.AutoHashMap(u32, void).init(self.allocator);
        defer skipDocs.deinit();

        try skipDocs.ensureTotalCapacity(@truncate(totalDocs / 10));

        for (blocks) |block| {
            skipDocs.clearRetainingCapacity();

            var docsIter = block.docs.iterator();
            while (docsIter.next()) |entry| {
                const docId = entry.key_ptr.*;
                const status = entry.value_ptr.*;
                if (!self.hasNewerVersion(docId, block.version)) {
                    try node.data.docs.put(docId, status);
                } else {
                    try skipDocs.put(docId, {});
                }
            }

            for (block.items.items) |item| {
                if (!skipDocs.contains(item.docId)) {
                    try node.data.items.append(item);
                }
            }
        }
    }

    std.sort.pdq(Item, node.data.items.items, {}, Item.cmp);

    committed = true;
    return merge;
}

fn checkBlocks(self: *Self) void {
    if (std.debug.runtime_safety) {
        var iter = self.blocks.first;
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

    log.debug("Adding in-memory block {}:{}", .{ merge.replacement.data.version, merge.replacement.data.merged });
    self.blocks.insertAfter(merge.last, merge.replacement);

    var iter: ?*Blocks.Node = merge.first;
    while (iter) |node| {
        iter = node.next;
        log.debug("Removing in-memory block {}:{}", .{ node.data.version, node.data.merged });
        self.blocks.remove(node);
        self.destroyNode(node);
        if (node == merge.last) break;
    }

    self.checkBlocks();
}

fn mergeBlocks(self: *Self) !void {
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

    var previousBlockVersion: u32 = 0;
    var blockIter = self.blocks.first;
    while (blockIter) |node| : (blockIter = node.next) {
        const block = &node.data;
        const items = block.items.items;

        if (deadline.isExpired()) {
            return error.Timeout;
        }

        assert(block.version > previousBlockVersion);
        previousBlockVersion = block.version;

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
                if (!r.found_existing or r.value_ptr.version < block.version) {
                    r.value_ptr.docId = docId;
                    r.value_ptr.score = 1;
                    r.value_ptr.version = block.version;
                } else if (r.value_ptr.version == block.version) {
                    r.value_ptr.score += 1;
                }
                previousHashEndedAt = i;
            }
        }

        // Remove results for docs that have been updated/deleted in the current block.
        // We can do it here, because we know previously processed blocks always have
        // lower version numbers.
        var resultIter = results.iterator();
        while (resultIter.next()) |result| {
            const version = result.value_ptr.version;
            if (version < block.version) {
                const docId = result.key_ptr.*;
                if (block.docs.contains(docId)) {
                    result.value_ptr.score = 0;
                    result.value_ptr.version = block.version;
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
