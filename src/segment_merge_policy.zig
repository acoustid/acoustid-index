const std = @import("std");
const log = std.log.scoped(.segment_merge_policy);

const verbose = false;

pub fn MergeCandidate(comptime Segment: type) type {
    return struct {
        start: *SegmentNode,
        end: *SegmentNode,
        num_segments: usize = 0,
        size: usize = 0,
        score: f64 = 0.0,

        const SegmentList = std.DoublyLinkedList(Segment);
        const SegmentNode = SegmentList.Node;
    };
}

pub fn TieredMergePolicy(comptime T: type) type {
    return struct {
        min_segment_size: usize = 100,
        max_segment_size: usize = 1_000_000_000,

        segments_per_merge: u32 = 10,
        segments_per_level: u32 = 10,

        const SegmentList = std.DoublyLinkedList(T);
        const SegmentNode = SegmentList.Node;

        pub const Candidate = MergeCandidate(T);

        const Self = @This();

        pub fn calculateBudget(self: Self, segments: SegmentList) usize {
            var total_size: usize = 0;
            var num_oversized_segments: usize = 0;
            var min_segment_size: usize = std.math.maxInt(usize);

            var iter = segments.first;
            while (iter) |node| : (iter = node.next) {
                const segment = &node.data;
                const size = segment.getSize();
                if (size > self.max_segment_size) {
                    num_oversized_segments += 1;
                    continue;
                }
                total_size += size;
                min_segment_size = @min(min_segment_size, size);
            }

            var floor_level = self.min_segment_size;
            var top_level = floor_level;
            const merge_factor = @min(self.segments_per_merge, self.segments_per_level);

            var num_allowed_segments: usize = 0;
            var level_size = floor_level;
            var remaining_size = total_size;
            while (true) {
                if (level_size < self.min_segment_size) {
                    floor_level = level_size;
                } else {
                    const segments_per_level = remaining_size * 100 / level_size;
                    if (segments_per_level < self.segments_per_level * 100 or level_size >= self.max_segment_size) {
                        num_allowed_segments += segments_per_level;
                        top_level = level_size;
                        break;
                    }
                    num_allowed_segments += self.segments_per_level * 100;
                    remaining_size -= self.segments_per_level * level_size;
                }
                level_size = @min(self.max_segment_size, level_size * merge_factor);
            }
            num_allowed_segments = (num_allowed_segments + 50) / 100;
            return num_allowed_segments + num_oversized_segments;
        }

        pub fn findSegmentsToMerge(self: Self, segments: SegmentList) ?Candidate {
            const num_segments = segments.len;
            const num_allowed_segments = self.calculateBudget(segments);
            log.debug("budget: {} segments", .{num_allowed_segments});

            if (num_allowed_segments >= segments.len) {
                return null;
            }

            const merge_factor = @min(self.segments_per_merge, self.segments_per_level);
            const log_merge_factor = @log2(@as(f64, @floatFromInt(merge_factor)));
            const log_min_segment_size = @log2(@as(f64, @floatFromInt(self.min_segment_size)));

            const tier_scaling_factor = @as(f64, @floatFromInt(num_allowed_segments)) / @as(f64, @floatFromInt(num_segments)) / @as(f64, @floatFromInt(self.segments_per_level));
            var tier = @as(f64, @floatFromInt(num_segments - 1)) * tier_scaling_factor;

            var best_candidate: ?Candidate = null;
            var best_score: f64 = 0.0;

            var max_merge_size: usize = self.max_segment_size * 2;

            var iter = segments.first;
            while (iter) |current_node| : (iter = current_node.next) {
                tier -= tier_scaling_factor;

                if (current_node.data.getSize() > self.max_segment_size) {
                    // skip oversized segments
                    continue;
                }

                // std.debug.print("evaluating segment {d} (size={d}, max_merge_size={}, tier={})\n", .{ current_node.data.id, current_node.data.getSize(), max_merge_size, tier });

                var candidate = Candidate{
                    .start = current_node,
                    .end = current_node,
                    .num_segments = 1,
                    .size = current_node.data.getSize(),
                };

                while (candidate.num_segments < self.segments_per_merge) {
                    const next_node = candidate.end.next orelse break;
                    const next_size = next_node.data.getSize();
                    candidate.end = next_node;
                    candidate.num_segments += 1;
                    candidate.size += next_size;

                    if (candidate.size > max_merge_size) {
                        break;
                    }

                    const log_size = @log2(@as(f64, @floatFromInt(candidate.size)));
                    const candidate_tier = (log_size - log_min_segment_size) / log_merge_factor;
                    const score = candidate_tier - tier;
                    // std.debug.print("candidate {}-{}: len={} size={} candidate_tier={}, score={d}\n", .{ candidate.start.data.id, candidate.end.data.id, candidate.num_segments, candidate.size, candidate_tier, score });
                    if (score < best_score or best_candidate == null) {
                        best_candidate = candidate;
                        best_score = score;
                    }

                    if (candidate.size > self.max_segment_size) {
                        // if we are over the max_segment_size setting, don't try to add more segments to the merge
                        break;
                    }
                }

                max_merge_size = current_node.data.getSize();
            }

            return best_candidate;
        }
    };
}

const MockSegment = struct {
    id: u64,
    size: usize,

    pub fn getSize(self: @This()) usize {
        return self.size;
    }
};

const MockSegmentList = std.DoublyLinkedList(MockSegment);

fn applyMerge(comptime T: type, segments: *std.DoublyLinkedList(T), merge: TieredMergePolicy(T).Candidate, allocator: std.mem.Allocator) !void {
    var iter = merge.start.next;
    while (iter) |node| {
        const next_node = node.next;
        merge.start.data.size += node.data.size;
        segments.remove(node);
        allocator.destroy(node);
        if (node == merge.end) break;
        iter = next_node orelse break;
    }
}

test "TieredMergePolicy" {
    var segments: std.DoublyLinkedList(MockSegment) = .{};

    defer {
        var iter = segments.first;
        while (iter) |node| {
            iter = node.next;
            std.testing.allocator.destroy(node);
        }
    }

    const policy = TieredMergePolicy(MockSegment){
        .min_segment_size = 100,
        .max_segment_size = 100000,
        .segments_per_merge = 10,
        .segments_per_level = 5,
    };

    var last_id: u64 = 1;

    var prng = std.rand.DefaultPrng.init(0);
    const rand = prng.random();

    for (0..10) |_| {
        var segment = try std.testing.allocator.create(MockSegmentList.Node);
        segment.data = .{ .id = last_id, .size = 100 + rand.intRangeAtMost(u16, 0, 200) };
        segments.append(segment);
        last_id += 1;
    }

    for (0..1000) |_| {
        if (verbose) {
            std.debug.print("---\n", .{});
        }

        if (rand.boolean()) {
            var segment = try std.testing.allocator.create(MockSegmentList.Node);
            segment.data = .{ .id = last_id, .size = 100 + rand.intRangeAtMost(u16, 0, 200) };
            segments.append(segment);
            last_id += 1;
        }

        if (verbose) {
            std.debug.print("segments:\n", .{});
            var iter = segments.first;
            while (iter) |node| {
                std.debug.print("  {}: {}\n", .{ node.data.id, node.data.size });
                iter = node.next;
            }
        }

        const candidate = policy.findSegmentsToMerge(segments) orelse continue;

        if (verbose) {
            std.debug.print("merging {}-{}\n", .{ candidate.start.data.id, candidate.end.data.id });
        }
        try applyMerge(MockSegment, &segments, candidate, std.testing.allocator);
    }

    const num_allowed_segmens = policy.calculateBudget(segments);
    try std.testing.expect(num_allowed_segmens >= segments.len);
}
