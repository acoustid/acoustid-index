const std = @import("std");

const common = @import("common.zig");
const SearchResults = common.SearchResults;

const SegmentMerger = @import("segment_merger.zig").SegmentMerger;
const TieredMergePolicy = @import("segment_merge_policy.zig").TieredMergePolicy;

const Deadline = @import("utils/Deadline.zig");

pub fn SegmentList(Segment: type) type {
    return struct {
        pub const Self = @This();
        pub const List = std.DoublyLinkedList(Segment);

        pub const MergePolicy = TieredMergePolicy(Segment);

        allocator: std.mem.Allocator,
        merge_policy: MergePolicy,
        segments: List,

        pub fn init(allocator: std.mem.Allocator, merge_policy: MergePolicy) Self {
            return .{
                .allocator = allocator,
                .merge_policy = merge_policy,
                .segments = .{},
            };
        }

        pub fn deinit(self: *Self) void {
            while (self.segments.popFirst()) |node| {
                self.destroySegment(node);
            }
        }

        // Creates a new segment and returns a pointer to the list node owning it.
        // This function is safe to call from any thread.
        pub fn createSegment(self: *Self) !*List.Node {
            const node = try self.allocator.create(List.Node);
            node.data = Segment.init(self.allocator);
            return node;
        }

        // Destroys a segment and frees the memory.
        // This function is safe to call from any thread, but only if the segment is not in the list.
        pub fn destroySegment(self: *Self, node: *List.Node) void {
            node.data.deinit();
            self.allocator.destroy(node);
        }

        pub fn removeAndDestroySegment(self: *Self, node: *List.Node) void {
            self.segments.remove(node);
            self.destroySegment(node);
        }

        pub fn appendSegment(self: *Self, node: *List.Node) void {
            self.segments.append(node);
        }

        pub fn getIdsAfterAppend(self: *Self, new_segment: *List.Node, allocator: std.mem.Allocator) !std.ArrayList(common.SegmentID) {
            var ids = std.ArrayList(common.SegmentID).init(allocator);
            errdefer ids.deinit();

            try ids.ensureTotalCapacity(self.segments.len + 1);

            var it = self.segments.first;
            while (it) |node| : (it = node.next) {
                ids.appendAssumeCapacity(node.data.id);
            }

            ids.appendAssumeCapacity(new_segment.data.id);

            return ids;
        }

        pub fn getIdsAfterAppliedMerge(self: *Self, merge: PreparedMerge, allocator: std.mem.Allocator) !std.ArrayList(common.SegmentID) {
            var ids = std.ArrayList(common.SegmentID).init(allocator);
            errdefer ids.deinit();

            try ids.ensureTotalCapacity(self.segments.len - merge.sources.num_segments + 1);

            var it = self.segments.first;
            var inside_merge = false;
            while (it) |node| : (it = node.next) {
                if (it == merge.sources.start) {
                    inside_merge = true;
                }
                if (!inside_merge) {
                    ids.appendAssumeCapacity(node.data.id);
                }
                if (it == merge.sources.end) {
                    inside_merge = false;
                    ids.appendAssumeCapacity(merge.target.data.id);
                }
            }

            return ids;
        }

        pub fn getMaxCommitId(self: *const Self) u64 {
            var max_commit_id: u64 = 0;
            var it = self.segments.first;
            while (it) |node| : (it = node.next) {
                if (node.data.max_commit_id > max_commit_id) {
                    max_commit_id = node.data.max_commit_id;
                }
            }
            return max_commit_id;
        }

        pub fn hasNewerVersion(self: *const Self, doc_id: u32, version: u32) bool {
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
            defer results.removeOutdatedResults(self);
            var iter = self.segments.first;
            while (iter) |node| : (iter = node.next) {
                if (deadline.isExpired()) {
                    return error.Timeout;
                }
                try node.data.search(hashes, results);
            }
        }

        pub const SegmentsToMerge = MergePolicy.Candidate;

        pub const PreparedMerge = struct {
            sources: SegmentsToMerge,
            target: *List.Node,
            merger: SegmentMerger(Segment),
        };

        pub fn prepareMerge(self: *Self) !?PreparedMerge {
            const sources = self.merge_policy.findSegmentsToMerge(self.segments) orelse return null;

            var merger = SegmentMerger(Segment).init(self.allocator, self);
            errdefer merger.deinit();

            var source_node = sources.start;
            while (true) {
                try merger.addSource(&source_node.data);
                if (source_node == sources.end) break;
                source_node = source_node.next orelse break;
            }
            try merger.prepare();

            const target = try self.createSegment();
            return .{ .sources = sources, .merger = merger, .target = target };
        }

        pub fn cleanupAfterMerge(self: *Self, merge: PreparedMerge, cleanup_args: anytype) void {
            var iter = merge.sources.start;
            while (true) {
                const next_node = iter.next;
                const is_end = iter == merge.sources.end;
                @call(.auto, Segment.cleanup, .{&iter.data} ++ cleanup_args);
                self.destroySegment(iter);
                if (is_end) break;
                iter = next_node orelse break;
            }
        }

        pub fn applyMerge(self: *Self, merge: PreparedMerge) void {
            self.segments.insertBefore(merge.sources.start, merge.target);
            var iter = merge.sources.start;
            while (true) {
                const next_node = iter.next;
                const is_end = iter == merge.sources.end;
                self.segments.remove(iter);
                if (is_end) break;
                iter = next_node orelse break;
            }
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
