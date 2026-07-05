// k-way merge of several segments into one. For each doc, the newest version
// wins (docs shadowed by a segment newer than their own are skipped, resolved
// via the collection's hasNewerVersion); deleted docs keep a tombstone in the
// merged docs map so they keep shadowing older versions. The result is a reader
// (read/advance + `.segment`) that filefmt.writeSegment can consume directly.

const std = @import("std");
const Item = @import("segment.zig").Item;
const SegmentInfo = @import("segment.zig").SegmentInfo;
const Metadata = @import("Metadata.zig");

pub const MergedSegmentInfo = struct {
    info: SegmentInfo = .{},
    metadata: Metadata,
    docs: std.AutoHashMapUnmanaged(u32, bool) = .{},
    min_doc_id: u32 = 0,
    max_doc_id: u32 = 0,

    pub fn deinit(self: *MergedSegmentInfo, allocator: std.mem.Allocator) void {
        self.metadata.deinit();
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
        sources: std.ArrayListUnmanaged(Source),
        segment: MergedSegmentInfo,
        estimated_size: usize = 0,
        current_item: ?Item = null,

        pub fn init(allocator: std.mem.Allocator, num_sources: usize) !Self {
            return .{
                .allocator = allocator,
                .sources = try std.ArrayListUnmanaged(Source).initCapacity(allocator, num_sources),
                .segment = .{ .metadata = Metadata.initOwned(allocator) },
            };
        }

        pub fn deinit(self: *Self) void {
            for (self.sources.items) |*source| source.deinit(self.allocator);
            self.sources.deinit(self.allocator);
            self.segment.deinit(self.allocator);
            self.* = undefined;
        }

        pub fn addSource(self: *Self, source: *Segment) void {
            self.sources.appendAssumeCapacity(.{ .reader = source.reader() });
        }

        /// `collection` must expose `hasNewerVersion(doc_id, version) bool`.
        pub fn prepare(self: *Self, collection: anytype) !void {
            const sources = self.sources.items;
            if (sources.len == 0) return error.NoSources;

            for (sources, 0..) |source, i| {
                if (i == 0) {
                    self.segment.info = source.reader.segment.info;
                } else {
                    self.segment.info = SegmentInfo.merge(self.segment.info, source.reader.segment.info);
                }
            }
            for (sources) |*source| {
                try self.segment.metadata.update(source.reader.segment.metadata);
            }

            var total_docs: u32 = 0;
            for (sources) |source| total_docs += source.reader.segment.docs.count();
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
                    if (!collection.hasNewerVersion(doc_id, segment.info.version)) {
                        try self.segment.docs.put(self.allocator, doc_id, doc_status);
                        docs_added += 1;
                        if (self.segment.min_doc_id == 0 or doc_id < self.segment.min_doc_id) self.segment.min_doc_id = doc_id;
                        if (self.segment.max_doc_id == 0 or doc_id > self.segment.max_doc_id) self.segment.max_doc_id = doc_id;
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
