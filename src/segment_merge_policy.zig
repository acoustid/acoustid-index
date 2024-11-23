const std = @import("std");
const log = std.log.scoped(.segment_merge_policy);

// This code is largely based on Michael McCandless' TieredMergePolicy from Lucene:
//   https://issues.apache.org/jira/browse/LUCENE-854
//   https://github.com/apache/lucene/blob/main/lucene/core/src/java/org/apache/lucene/index/TieredMergePolicy.java
//
// The main difference is that we only merge adjacent segments, and restrict
// merges, so that we always maintain the sorting order.
//
// The original code is licensed under this license:
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

            var best_candidate: ?Candidate = null;
            var best_score: f64 = 0.0;

            var max_merge_size: usize = self.max_segment_size * 2;

            var iter = segments.first;
            while (iter) |first_segment| : (iter = first_segment.next) {
                const first_segment_size = first_segment.data.getSize();
                if (first_segment_size > self.max_segment_size) {
                    // Skip oversized segments that can't be further merged
                    continue;
                }

                var candidate = Candidate{
                    .start = first_segment,
                    .end = first_segment,
                    .num_segments = 1,
                    .size = first_segment_size,
                };

                while (candidate.num_segments < self.segments_per_merge) {
                    const next_node = candidate.end.next orelse break;
                    const next_size = next_node.data.getSize();
                    candidate.end = next_node;
                    candidate.num_segments += 1;
                    candidate.size += next_size;

                    if (candidate.size > max_merge_size) {
                        // This merge would break segment ordering
                        break;
                    }

                    // Roughly measure "skew" of the merge, i.e. how
                    // "balanced" the merge is (whether the segments are
                    // about the same size), which can range from
                    // 1.0/numSegsBeingMerged (good) to 1.0 (poor). Heavily
                    // lopsided merges (skew near 1.0) is no good; it means
                    // O(N^2) merge cost over time:
                    var skew: f64 = undefined;
                    if (candidate.size > self.max_segment_size) {
                        // Pretend the merge has perfect skew; skew doesn't
                        // matter in this case because this merge will not
                        // "cascade" and so it cannot lead to N^2 merge cost
                        // over time:
                        skew = 1.0 / @as(f64, @floatFromInt(self.segments_per_merge));
                    } else {
                        skew = @as(f64, @floatFromInt(first_segment_size)) / @as(f64, @floatFromInt(candidate.size));
                    }

                    // Strongly favor merges with less skew (smaller
                    // score is better):
                    var score = skew;

                    // Gently favor smaller merges over bigger ones. We
                    // don't want to make this exponent too large else we
                    // can end up doing poor merges of small segments in
                    // order to avoid the large merges
                    score *= std.math.pow(f64, @floatFromInt(candidate.size), 0.05);

                    if (score < best_score or best_candidate == null) {
                        best_candidate = candidate;
                        best_score = score;
                    }

                    // If we are over the max_segment_size setting, don't try to add more segments to the merge.
                    if (candidate.size > self.max_segment_size) {
                        break;
                    }
                }

                max_merge_size = first_segment.data.getSize();
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
