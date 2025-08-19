const std = @import("std");

const Item = @import("segment.zig").Item;
const SegmentInfo = @import("segment.zig").SegmentInfo;
const SegmentList = @import("segment_list.zig").SegmentList;
const SharedPtr = @import("utils/shared_ptr.zig").SharedPtr;

pub const MergedSegmentInfo = struct {
    info: SegmentInfo = .{},
    metadata: std.StringHashMapUnmanaged(?[]const u8) = .{},
    docs: std.AutoHashMapUnmanaged(u32, bool) = .{},
    min_doc_id: u32 = 0,
    max_doc_id: u32 = 0,

    pub fn deinit(self: *MergedSegmentInfo, allocator: std.mem.Allocator) void {
        var iter = self.metadata.iterator();
        while (iter.next()) |e| {
            allocator.free(e.key_ptr.*);
            if (e.value_ptr.*) |value| {
                allocator.free(value);
            }
        }
        self.metadata.deinit(allocator);
        self.docs.deinit(allocator);
    }
};

pub fn SegmentMerger(comptime Segment: type) type {
    return struct {
        const Self = @This();

        const Source = struct {
            reader: Segment.Reader,
            skip_docs: std.AutoHashMapUnmanaged(u32, void) = .{},

            pub fn deinit(self: *Source, allocator: std.mem.Allocator) void {
                self.reader.close();
                self.skip_docs.deinit(allocator);
            }

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
        collection: *SegmentList(Segment),
        sources: std.ArrayListUnmanaged(Source) = .{},
        segment: MergedSegmentInfo = .{},
        estimated_size: usize = 0,

        current_item: ?Item = null,

        pub fn init(allocator: std.mem.Allocator, collection: *SegmentList(Segment), num_sources: usize) !Self {
            return .{
                .allocator = allocator,
                .collection = collection,
                .sources = try std.ArrayListUnmanaged(Source).initCapacity(allocator, num_sources),
            };
        }

        pub fn deinit(self: *Self) void {
            for (self.sources.items) |*source| {
                source.deinit(self.allocator);
            }
            self.sources.deinit(self.allocator);
            self.segment.deinit(self.allocator);
            self.* = undefined;
        }

        pub fn addSource(self: *Self, source: *Segment) void {
            self.sources.appendAssumeCapacity(.{
                .reader = source.reader(),
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
                total_attributes += source.reader.segment.metadata.count();
                total_docs += source.reader.segment.docs.count();
            }

            try self.segment.metadata.ensureTotalCapacity(self.allocator, total_attributes);
            for (sources) |*source| {
                const segment = source.reader.segment;
                var iter = segment.metadata.iterator();
                while (iter.next()) |entry| {
                    const name = entry.key_ptr.*;
                    const value = entry.value_ptr.*;
                    const result = self.segment.metadata.getOrPutAssumeCapacity(name);
                    if (!result.found_existing) {
                        errdefer self.segment.metadata.removeByPtr(result.key_ptr);
                        result.key_ptr.* = try self.allocator.dupe(u8, name);
                    }
                    if (value) |v| {
                        result.value_ptr.* = try self.allocator.dupe(u8, v);
                    } else {
                        result.value_ptr.* = null;
                    }
                }
            }

            try self.segment.docs.ensureTotalCapacity(self.allocator, total_docs);
            self.segment.min_doc_id = 0;
            self.segment.max_doc_id = 0;
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
                        if (self.segment.min_doc_id == 0 or doc_id < self.segment.min_doc_id) {
                            self.segment.min_doc_id = doc_id;
                        }
                        if (self.segment.max_doc_id == 0 or doc_id > self.segment.max_doc_id) {
                            self.segment.max_doc_id = doc_id;
                        }
                    } else {
                        try source.skip_docs.put(self.allocator, doc_id, {});
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
                        if (min_item == null or Item.order(item, min_item.?) == .lt) {
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

    var merger = try SegmentMerger(MemorySegment).init(std.testing.allocator, &collection, 3);
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

    merger.addSource(node1.value);
    merger.addSource(node2.value);
    merger.addSource(node3.value);

    try merger.prepare();

    try std.testing.expectEqualDeep(SegmentInfo{ .version = 11, .merges = 2 }, merger.segment.info);

    while (true) {
        const item = try merger.read();
        if (item == null) {
            break;
        }
    }
}
