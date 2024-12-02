const std = @import("std");

const Item = @import("segment.zig").Item;
const SegmentInfo = @import("segment.zig").SegmentInfo;
const SegmentList = @import("segment_list.zig").SegmentList;
const SharedPtr = @import("utils/shared_ptr.zig").SharedPtr;

pub const MergedSegmentInfo = struct {
    info: SegmentInfo = .{},
    attributes: std.AutoHashMapUnmanaged(u64, u64) = .{},
    docs: std.AutoHashMapUnmanaged(u32, bool) = .{},
};

pub fn SegmentMerger(comptime Segment: type) type {
    return struct {
        const Self = @This();

        const Source = struct {
            reader: Segment.Reader,
            skip_docs: std.AutoHashMap(u32, void),

            pub fn read(self: *Source) !?Item {
                while (true) {
                    const item = try self.reader.read() orelse return null;
                    if (self.skip_docs.contains(item.id)) {
                        self.reader.advance();
                        continue;
                    }
                    return item;
                }
            }

            pub fn advance(self: *Source) void {
                self.reader.advance();
            }
        };

        allocator: std.mem.Allocator,
        sources: std.ArrayList(Source),
        collection: *SegmentList(Segment),
        segment: MergedSegmentInfo = .{},
        estimated_size: usize = 0,

        current_item: ?Item = null,

        pub fn init(allocator: std.mem.Allocator, collection: *SegmentList(Segment)) Self {
            return .{
                .allocator = allocator,
                .sources = std.ArrayList(Source).init(allocator),
                .collection = collection,
            };
        }

        pub fn deinit(self: *Self) void {
            for (self.sources.items) |*source| {
                source.reader.close();
                source.skip_docs.deinit();
            }
            self.sources.deinit();
            self.segment.docs.deinit(self.allocator);
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

            var total_attributes: u32 = 0;
            var total_docs: u32 = 0;
            for (sources, 0..) |source, i| {
                if (i == 0) {
                    self.segment.info = source.reader.segment.info;
                } else {
                    self.segment.info = SegmentInfo.merge(self.segment.info, source.reader.segment.info);
                }
                total_attributes += source.reader.segment.attributes.count();
                total_docs += source.reader.segment.docs.count();
            }

            try self.segment.attributes.ensureTotalCapacity(self.allocator, total_attributes);
            for (sources) |*source| {
                const segment = source.reader.segment;
                var iter = segment.attributes.iterator();
                while (iter.next()) |entry| {
                    const key = entry.key_ptr.*;
                    const value = entry.value_ptr.*;
                    self.segment.attributes.putAssumeCapacity(key, value);
                }
            }

            try self.segment.docs.ensureTotalCapacity(self.allocator, total_docs);
            for (sources) |*source| {
                const segment = source.reader.segment;
                var docs_added: usize = 0;
                var docs_found: usize = 0;
                var iter = segment.docs.iterator();
                while (iter.next()) |entry| {
                    docs_found += 1;
                    const doc_id = entry.key_ptr.*;
                    const doc_status = entry.value_ptr.*;
                    if (!self.collection.hasNewerVersion(doc_id, segment.info.version)) {
                        try self.segment.docs.put(self.allocator, doc_id, doc_status);
                        docs_added += 1;
                    } else {
                        try source.skip_docs.put(doc_id, {});
                    }
                }
                if (docs_found > 0) {
                    const ratio = (100 * docs_added) / docs_found;
                    self.estimated_size += segment.getSize() * @min(100, ratio + 10) / 100;
                }
            }
        }

        pub fn read(self: *Self) !?Item {
            if (self.current_item == null) {
                var min_item: ?Item = null;
                var min_item_index: usize = 0;

                for (self.sources.items, 0..) |*source, i| {
                    if (try source.read()) |item| {
                        if (min_item == null or Item.cmp({}, item, min_item.?)) {
                            min_item = item;
                            min_item_index = i;
                        }
                    }
                }

                if (min_item) |item| {
                    self.sources.items[min_item_index].advance();
                    self.current_item = item;
                }
            }
            return self.current_item;
        }

        pub fn advance(self: *Self) void {
            self.current_item = null;
        }
    };
}

test "merge segments" {
    const MemorySegment = @import("MemorySegment.zig");

    var collection = try SegmentList(MemorySegment).init(std.testing.allocator, 3);
    defer collection.deinit(std.testing.allocator, .delete);

    var merger = SegmentMerger(MemorySegment).init(std.testing.allocator, &collection);
    defer merger.deinit();

    var node1 = try SegmentList(MemorySegment).createSegment(std.testing.allocator, .{});
    collection.nodes.appendAssumeCapacity(node1);

    var node2 = try SegmentList(MemorySegment).createSegment(std.testing.allocator, .{});
    collection.nodes.appendAssumeCapacity(node2);

    var node3 = try SegmentList(MemorySegment).createSegment(std.testing.allocator, .{});
    collection.nodes.appendAssumeCapacity(node3);

    node1.value.info = .{ .version = 11, .merges = 0 };
    node2.value.info = .{ .version = 12, .merges = 0 };
    node3.value.info = .{ .version = 13, .merges = 0 };

    try merger.addSource(node1.value);
    try merger.addSource(node2.value);
    try merger.addSource(node3.value);

    try merger.prepare();

    try std.testing.expectEqualDeep(SegmentInfo{ .version = 11, .merges = 2 }, merger.segment.info);

    while (true) {
        const item = try merger.read();
        if (item == null) {
            break;
        }
    }
}
