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
        max_segments: ?usize = null,

        min_segment_size: usize = 100,
        max_segment_size: usize = 1_000_000_000,

        segments_per_merge: u32 = 10,
        segments_per_level: u32 = 10,

        strategy: Strategy = .balanced,

        const Strategy = enum {
            balanced,
            aggressive,
        };

        const SegmentList = std.DoublyLinkedList(T);
        const SegmentNode = SegmentList.Node;

        pub const Candidate = MergeCandidate(T);

        const Self = @This();

        pub const CalculateBudgetResult = struct {
            floor_level: usize,
            top_level: usize,
            total_size: usize,
            num_allowed_segments: usize,
        };

        pub fn calculateBudget(self: Self, segments: SegmentList) CalculateBudgetResult {
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
            const merge_factor = @max(2, @min(self.segments_per_merge, self.segments_per_level));

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
            num_allowed_segments = self.max_segments orelse (num_allowed_segments + 50) / 100;
            return .{
                .num_allowed_segments = num_allowed_segments + num_oversized_segments,
                .floor_level = floor_level,
                .top_level = top_level,
                .total_size = total_size,
            };
        }

        pub const FindSegmentsToMergeResult = struct {
            num_allowed_segments: usize,
            candidate: ?Candidate,
        };

        pub fn findSegmentsToMerge(self: Self, segments: SegmentList) FindSegmentsToMergeResult {
            const stats = self.calculateBudget(segments);
            log.debug("budget: {} segments", .{stats.num_allowed_segments});

            var result = FindSegmentsToMergeResult{
                .num_allowed_segments = stats.num_allowed_segments,
                .candidate = null,
            };

            const num_segments = segments.len;
            if (stats.num_allowed_segments >= num_segments) {
                return result;
            }

            //const merge_factor = @max(2, @min(self.segments_per_merge, self.segments_per_level));
            // const log_merge_factor = @log2(@as(f64, @floatFromInt(merge_factor)));
            // const log_min_segment_size = @log2(@as(f64, @floatFromInt(self.min_segment_size)));

            const tier_scaling_factor = @as(f64, @floatFromInt(stats.num_allowed_segments)) / @as(f64, @floatFromInt(num_segments)) / @as(f64, @floatFromInt(self.segments_per_level));
            const top_tier = @as(f64, @floatFromInt(num_segments)) * tier_scaling_factor;
            var tier = top_tier;

            var segment_no: usize = 0;

            var best_candidate: ?Candidate = null;
            var best_score: f64 = 0.0;

            var max_merge_size: usize = self.max_segment_size * 2;

            var iter = segments.first;
            while (iter) |current_node| : (iter = current_node.next) {
                tier -= tier_scaling_factor;
                segment_no += 1;

                if (current_node.data.getSize() > self.max_segment_size) {
                    // skip oversized segments
                    continue;
                }

                var target_merge_size = max_merge_size;

                if (target_merge_size > self.max_segment_size) {
                    target_merge_size = current_node.data.getSize() * self.segments_per_merge;
                }

                if (verbose) {
                    std.debug.print("evaluating segment {d} (no={}, size={d}, target_merge_size={})\n", .{ current_node.data.id, segment_no, current_node.data.getSize(), target_merge_size });
                }

                const current_node_size = current_node.data.getSize();

                var candidate = Candidate{
                    .start = current_node,
                    .end = current_node,
                    .num_segments = 1,
                    .size = current_node_size,
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

                    // Roughly measure "skew" of the merge, i.e. how
                    // "balanced" the merge is (whether the segments are
                    // about the same size), which can range from
                    // 1.0/numSegsBeingMerged (good) to 1.0 (poor). Heavily
                    // lopsided merges (skew near 1.0) is no good; it means
                    // O(N^2) merge cost over time:
                    const skew = @as(f64, @floatFromInt(current_node_size)) / @as(f64, @floatFromInt(candidate.size));

                    // Strongly favor merges with less skew (smaller
                    // score is better):
                    var score = skew;

                    // Gently favor smaller merges over bigger ones. We
                    // don't want to make this exponent too large else we
                    // can end up doing poor merges of small segments in
                    // order to avoid the large merges
                    score *= std.math.pow(f64, @floatFromInt(candidate.size), 0.05);

                    //std.debug.print("  candidate {}-{} (size={}, len={})\n", .{ candidate.start.data.id, candidate.end.data.id, candidate.size, candidate.num_segments });

                    // const log_size = @log2(@as(f64, @floatFromInt(candidate.size)));
                    // const candidate_tier = (log_size - log_min_segment_size) / log_merge_factor;
                    // var score = candidate_tier - tier;

                    // const adjustment_factor: f64 = switch (self.strategy) {
                    //     .balanced => 1.2,
                    //     .aggressive => 1.8,
                    // };

                    // const adjustment = @as(f64, @floatFromInt(candidate.num_segments)) / @as(f64, @floatFromInt(self.segments_per_merge));
                    // score = score - adjustment_factor * adjustment;

                    //const max_merge_size_f: f64 = @floatFromInt(max_merge_size);
                    //const target_merge_size_f: f64 = @floatFromInt(target_merge_size);
                    //const candidate_size_f: f64 = @floatFromInt(candidate.size);

                    //const next_target_merge_size_f = target_merge_size_f / @as(f64, @floatFromInt(self.segments_per_merge));

                    //const score_closer_to_current_target_merge_size = @abs(target_merge_size_f - candidate_size_f) / target_merge_size_f;
                    //const score_closer_to_next_target_merge_size = @abs(next_target_merge_size_f - candidate_size_f) / target_merge_size_f;

                    //const num_segments_f = @as(f64, @floatFromInt(candidate.num_segments));
                    //const avg_segment_size_f: f64 = candidate_size_f / num_segments_f;
                    //const first_segment_size_f: f64 = @as(f64, @floatFromInt(current_node.data.getSize()));
                    //std.debug.print("    avg_segment_size={d}\n", .{avg_segment_size_f});
                    //const distance_to_avg_segment_size = @abs(first_segment_size_f - avg_segment_size_f) / first_segment_size_f;
                    //std.debug.print("    distance_to_avg_segment_size={d}\n", .{distance_to_avg_segment_size});

                    //const distance_to_target_merge_size = score_closer_to_current_target_merge_size;
                    //std.debug.print("    score_closer_to_target_merge_size={d}\n", .{distance_to_target_merge_size});

                    //const score_bigger_merge = 1 - @as(f64, @floatFromInt(candidate.num_segments)) / @as(f64, @floatFromInt(self.segments_per_merge));
                    // std.debug.print("    score_bigger_merge={d}\n", .{score_bigger_merge});

                    //const score_smaller_segment_no = 1 - @as(f64, @floatFromInt(segment_no)) / @as(f64, @floatFromInt(num_segments));
                    //std.debug.print("    score_smaller_segment_no={d}\n", .{score_smaller_segment_no});

                    //const score_oversized = candidate_size_f / @as(f64, @floatFromInt(stats.total_size));
                    //                    const score_oversized = if (candidate.size < self.max_segment_size) 0.0 else (max_merge_size_f - candidate_size_f) / max_merge_size_f;
                    // std.debug.print("    score_oversized={d}\n", .{score_oversized});

                    //const score = score_bigger_merge * 0.5 + score_oversized * 0.5;
                    //score = score * 0.5 + score_oversized * 0.5;

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

            result.candidate = best_candidate;
            return result;
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
        .strategy = .aggressive,
    };

    var last_id: u64 = 1;

    var prng = std.rand.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        try std.posix.getrandom(std.mem.asBytes(&seed));
        //seed = 16044660244849477186;
        if (verbose) {
            std.debug.print("seed={}\n", .{seed});
        }
        break :blk seed;
    });
    const rand = prng.random();

    for (0..10) |_| {
        var segment = try std.testing.allocator.create(MockSegmentList.Node);
        segment.data = .{ .id = last_id, .size = 100 + rand.intRangeAtMost(u16, 0, 200) };
        segments.append(segment);
        last_id += 1;
    }

    var total_merge_size: u64 = 0;
    var total_merge_count: u64 = 0;

    for (0..1000) |i| {
        if (verbose) {
            std.debug.print("--- [{}] ---\n", .{i});
        }

        if (rand.boolean() or true) {
            var segment = try std.testing.allocator.create(MockSegmentList.Node);
            segment.data = .{ .id = last_id, .size = rand.intRangeAtMost(u16, 100, 200) };
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

        const result = policy.findSegmentsToMerge(segments);
        const candidate = result.candidate orelse continue;

        total_merge_size += candidate.num_segments;
        total_merge_count += 1;

        if (verbose) {
            std.debug.print("merging {}-{}\n", .{ candidate.start.data.id, candidate.end.data.id });
        }
        try applyMerge(MockSegment, &segments, candidate, std.testing.allocator);
    }

    if (verbose) {
        std.debug.print("num merges: {}\n", .{total_merge_count});
        std.debug.print("avg merge size: {}\n", .{total_merge_size / total_merge_count});
    }

    const s = policy.calculateBudget(segments);
    try std.testing.expect(s.num_allowed_segments >= segments.len);
}
