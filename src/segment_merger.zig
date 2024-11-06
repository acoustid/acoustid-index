const std = @import("std");

const common = @import("common.zig");
const Item = common.Item;
const SegmentID = common.SegmentID;

pub const MergedSegmentInfo = struct {
    id: SegmentID,
    docs: std.AutoHashMap(u32, bool),
    max_commit_id: u64,
};

pub fn SegmentMerger(comptime Segment: type) type {
    return struct {
        const Self = @This();

        const Source = struct {
            reader: Segment.Reader,
            skip_docs: std.AutoHashMap(u32, void),
            item: ?Item = null,
            has_more: bool = true,

            pub fn load(self: *Source) !void {
                while (self.item == null and self.has_more) {
                    try self.reader.load();
                    if (self.reader.item) |item| {
                        if (self.skip_docs.contains(item.id)) {
                            self.reader.item = null;
                            continue;
                        } else {
                            self.reader.item = null;
                            self.item = item;
                            return;
                        }
                    } else {
                        self.has_more = false;
                        return;
                    }
                }
            }
        };

        allocator: std.mem.Allocator,
        sources: std.ArrayList(Source),
        collection: *Segment.List,
        segment: MergedSegmentInfo,

        pub fn init(allocator: std.mem.Allocator, collection: *Segment.List) Self {
            return .{
                .allocator = allocator,
                .sources = std.ArrayList(Source).init(allocator),
                .collection = collection,
                .segment = .{
                    .id = .{ .version = 0, .included_merges = 0 },
                    .docs = std.AutoHashMap(u32, bool).init(allocator),
                    .max_commit_id = 0,
                },
            };
        }

        pub fn deinit(self: *Self) void {
            for (self.sources.items) |*source| {
                source.reader.close();
                source.skip_docs.deinit();
            }
            self.sources.deinit();
            self.* = undefined;
        }

        pub fn addSource(self: *Self, source: *Segment) !void {
            try self.sources.append(.{
                .reader = source.reader(),
                .skip_docs = std.AutoHashMap(u32, void).init(self.allocator),
            });
        }

        pub fn prepare(self: *Self) !void {
            const sources = self.sources.items;
            if (sources.len == 0) {
                return error.NoSources;
            }

            var total_docs: u32 = 0;
            for (sources, 0..) |source, i| {
                if (i == 0) {
                    self.segment.id = source.reader.segment.id;
                    self.segment.max_commit_id = source.reader.segment.max_commit_id;
                } else {
                    self.segment.id = SegmentID.merge(self.segment.id, source.reader.segment.id);
                    self.segment.max_commit_id = @max(self.segment.max_commit_id, source.reader.segment.max_commit_id);
                }
                total_docs += source.reader.segment.docs.count();
            }

            try self.segment.docs.ensureTotalCapacity(total_docs);
            for (sources) |*source| {
                var docs_iter = source.reader.segment.docs.iterator();
                while (docs_iter.next()) |entry| {
                    const doc_id = entry.key_ptr.*;
                    const doc_status = entry.value_ptr.*;
                    if (!self.collection.hasNewerVersion(doc_id, source.reader.segment.id.version)) {
                        try self.segment.docs.put(doc_id, doc_status);
                    } else {
                        try source.skip_docs.put(doc_id, {});
                    }
                }
            }
        }

        pub fn read(self: *Self) !?Item {
            for (self.sources.items) |*source| {
                try source.load();
            }

            var next_item: ?Item = null;
            for (self.sources.items) |*source| {
                if (source.item) |item| {
                    if (next_item == null or Item.cmp({}, item, next_item.?)) {
                        next_item = item;
                        source.item = null;
                    }
                }
            }
            return next_item;
        }
    };
}

test "merge segments" {
    const InMemorySegment = @import("InMemorySegment.zig");

    var collection = InMemorySegment.List.init(std.testing.allocator);
    defer collection.deinit();

    var merger = SegmentMerger(InMemorySegment).init(std.testing.allocator, &collection);
    defer merger.deinit();

    var node1 = try collection.createSegment();
    collection.segments.append(node1);

    var node2 = try collection.createSegment();
    collection.segments.append(node2);

    node1.data.id = SegmentID{ .version = 1, .included_merges = 0 };
    node1.data.max_commit_id = 11;

    node2.data.id = SegmentID{ .version = 2, .included_merges = 0 };
    node2.data.max_commit_id = 12;

    try merger.addSource(&node1.data);
    try merger.addSource(&node2.data);

    try merger.prepare();

    try std.testing.expectEqual(1, merger.segment.id.version);
    try std.testing.expectEqual(1, merger.segment.id.included_merges);
    try std.testing.expectEqual(12, merger.segment.max_commit_id);

    while (true) {
        const item = try merger.read();
        if (item == null) {
            break;
        }
    }
}
