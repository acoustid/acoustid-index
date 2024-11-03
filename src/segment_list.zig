const std = @import("std");

const common = @import("common.zig");
const SearchResults = common.SearchResults;

const Deadline = @import("utils/Deadline.zig");

pub fn SegmentList(Segment: type) type {
    return struct {
        pub const Self = @This();
        pub const List = std.DoublyLinkedList(Segment);

        allocator: std.mem.Allocator,
        segments: List,

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .segments = .{},
            };
        }

        pub fn deinit(self: *Self) void {
            while (self.segments.popFirst()) |node| {
                self.destroySegment(node);
            }
        }

        pub fn createSegment(self: *Self) !*List.Node {
            const node = try self.allocator.create(List.Node);
            node.data = Segment.init(self.allocator);
            return node;
        }

        pub fn destroySegment(self: *Self, node: *List.Node) void {
            node.data.deinit();
            self.allocator.destroy(node);
        }

        pub fn removeAndDestroy(self: *Self, node: *List.Node) void {
            self.segments.remove(node);
            self.destroySegment(node);
        }

        pub fn getIds(self: *Self, ids: *std.ArrayList(common.SegmentID)) !void {
            try ids.ensureTotalCapacity(self.segments.len);
            var it = self.segments.first;
            while (it) |node| : (it = node.next) {
                try ids.append(node.data.id);
            }
        }

        pub fn getMaxCommitId(self: *Self) u64 {
            var max_commit_id: u64 = 0;
            var it = self.segments.first;
            while (it) |node| : (it = node.next) {
                if (node.data.max_commit_id > max_commit_id) {
                    max_commit_id = node.data.max_commit_id;
                }
            }
            return max_commit_id;
        }

        pub fn hasNewerVersion(self: *Self, doc_id: u32, version: u32) bool {
            var it = self.segments.last;
            while (it) |node| : (it = node.prev) {
                if (node.data.id.version > version) {
                    if (node.data.docs.contains(doc_id)) {
                        return true;
                    }
                } else {
                    break;
                }
            }
            return false;
        }

        pub fn search(self: *Self, hashes: []const u32, results: *SearchResults, deadline: Deadline) !void {
            std.debug.assert(std.sort.isSorted(u32, hashes, {}, std.sort.asc(u32)));
            var it = self.segments.first;
            while (it) |node| : (it = node.next) {
                if (deadline.isExpired()) {
                    return error.Timeout;
                }
                try node.data.search(hashes, results);
            }
        }

        pub const SegmentsToMerge = struct {
            node1: *List.Node,
            node2: *List.Node,
        };

        pub fn findSegmentsToMerge(self: *Self, options: SegmentMergeOptions) ?SegmentsToMerge {
            var total_size: usize = 0;
            var max_size: usize = 0;
            var min_size: usize = std.math.maxInt(usize);
            var num_segments: usize = 0;
            var segments_iter = self.segments.first;
            while (segments_iter) |node| : (segments_iter = node.next) {
                if (!node.data.canBeMerged())
                    continue;
                const size = node.data.getSize();
                if (size >= options.max_segment_size)
                    continue;
                num_segments += 1;
                total_size += size;
                max_size = @max(max_size, size);
                min_size = @min(min_size, size);
            }

            if (total_size == 0) {
                return null;
            }

            const max_segments = options.getMaxSegments(total_size);
            if (num_segments < max_segments) {
                return null;
            }

            var best_node: ?*List.Node = null;
            var best_score: f64 = std.math.inf(f64);
            segments_iter = self.segments.first;
            var level_size = @as(f64, @floatFromInt(total_size)) / 2;
            while (segments_iter) |node| : (segments_iter = node.next) {
                if (!node.data.canBeMerged())
                    continue;
                const size = node.data.getSize();
                if (size >= options.max_segment_size)
                    continue;
                if (node.next) |next_node| {
                    const merge_size = size + next_node.data.getSize();
                    const score = @as(f64, @floatFromInt(merge_size)) - level_size;
                    if (score < best_score) {
                        best_node = node;
                        best_score = score;
                    }
                }
                level_size /= 2;
            }

            if (best_node) |node| {
                if (node.next) |next_node| {
                    return .{ .node1 = node, .node2 = next_node };
                }
            }
            return null;
        }

        pub const PreparedMerge = struct {
            sources: SegmentsToMerge,
            target: *List.Node,
        };

        pub fn prepareMerge(self: *Self, options: SegmentMergeOptions) !?PreparedMerge {
            const sources_opt = self.findSegmentsToMerge(options);
            if (sources_opt) |sources| {
                const target = try self.createSegment();
                return .{ .sources = sources, .target = target };
            }
            return null;
        }

        pub fn applyMerge(self: *Self, merge: PreparedMerge) void {
            self.segments.insertBefore(merge.sources.node1, merge.target);
            self.segments.remove(merge.sources.node1);
            self.segments.remove(merge.sources.node2);
        }

        pub fn revertMerge(self: *Self, merge: PreparedMerge) void {
            self.segments.insertBefore(merge.target, merge.sources.node1);
            self.segments.insertBefore(merge.target, merge.sources.node2);
            self.segments.remove(merge.target);
        }

        pub fn destroyMergedSegments(self: *Self, merge: PreparedMerge) void {
            self.destroySegment(merge.sources.node1);
            self.destroySegment(merge.sources.node2);
        }
    };
}

pub const SegmentMergeOptions = struct {
    max_segment_size: usize,

    pub fn getMaxSegments(self: SegmentMergeOptions, total_size: usize) usize {
        const max_level_size = @min(self.max_segment_size, @max(total_size / 2, 10));
        const min_level_size = @max(max_level_size / 1000, 10);
        const x = max_level_size / min_level_size;
        if (x == 0) {
            return 1;
        } else {
            return @max(1, std.math.log2_int(usize, x));
        }
    }
};
