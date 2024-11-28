const std = @import("std");
const log = std.log.scoped(.segment_merge_policy);

const SharedPtr = @import("utils/smartptr.zig").SharedPtr;

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

pub const MergeCandidate = struct {
    start: usize,
    end: usize,
    size: usize = 0,
    score: f64 = 0.0,
};

pub fn TieredMergePolicy(comptime Segment: type, comptime getSizeFn: anytype) type {
    return struct {
        max_segments: ?usize = null,

        min_segment_size: usize = 100,
        max_segment_size: usize = 1_000_000_000,

        segments_per_merge: u32 = 10,
        segments_per_level: u32 = 10,

        const Self = @This();

        pub fn calculateBudget(self: Self, segments: []Segment) usize {
            var total_size: usize = 0;
            var num_oversized_segments: usize = 0;

            for (segments) |segment| {
                const size = getSizeFn(segment);
                if (size > self.max_segment_size) {
                    num_oversized_segments += 1;
                    continue;
                }
                total_size += size;
            }

            if (self.max_segments) |num_allowed_segments| {
                return num_allowed_segments + num_oversized_segments;
            }

            const merge_factor = @max(2, @min(self.segments_per_merge, self.segments_per_level));

            var num_allowed_segments: usize = 0;
            var level_size = self.min_segment_size;
            var remaining_size = total_size;
            while (true) {
                const segments_per_level = remaining_size * 100 / level_size;
                if (segments_per_level < self.segments_per_level * 100 or level_size >= self.max_segment_size) {
                    num_allowed_segments += segments_per_level;
                    break;
                }
                num_allowed_segments += self.segments_per_level * 100;
                remaining_size -= self.segments_per_level * level_size;
                level_size = @min(self.max_segment_size, level_size * merge_factor);
            }
            num_allowed_segments = (num_allowed_segments + 50) / 100;
            return num_allowed_segments + num_oversized_segments;
        }

        pub const FindSegmentsToMergeResult = struct {
            num_allowed_segments: usize,
            candidate: ?MergeCandidate,
        };

        pub fn findSegmentsToMerge(self: Self, segments: []Segment) ?MergeCandidate {
            var best_candidate: ?MergeCandidate = null;
            var best_score: f64 = 0.0;

            var max_merge_size: usize = self.max_segment_size * 2;

            var start: usize = 0;
            while (start + 1 < segments.len) : (start += 1) {
                const start_size = getSizeFn(segments[start]);
                if (start_size > self.max_segment_size) {
                    // Skip oversized segments that can't be further merged
                    continue;
                }

                var candidate = MergeCandidate{
                    .start = start,
                    .end = start,
                    .size = 0,
                };

                while (candidate.end < segments.len) {
                    candidate.size += getSizeFn(segments[candidate.end]);
                    candidate.end += 1;

                    if (candidate.end - candidate.start > self.segments_per_merge or candidate.size > max_merge_size) {
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
                        skew = @as(f64, @floatFromInt(start_size)) / @as(f64, @floatFromInt(candidate.size));
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

                max_merge_size = start_size;
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

fn applyMerge(segments: *std.ArrayList(MockSegment), merge: MergeCandidate) !void {
    for (segments.items[merge.start + 1 .. merge.end]) |seg| {
        segments.items[merge.start].size += seg.size;
    }
    segments.replaceRangeAssumeCapacity(merge.start + 1, merge.end - merge.start - 1, &.{});
}

test "TieredMergePolicy" {
    var segments = std.ArrayList(MockSegment).init(std.testing.allocator);
    defer segments.deinit();

    const policy = TieredMergePolicy(MockSegment, MockSegment.getSize){
        .min_segment_size = 100,
        .max_segment_size = 100000,
        .segments_per_merge = 10,
        .segments_per_level = 5,
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
        const segment = MockSegment{ .id = last_id, .size = 100 + rand.intRangeAtMost(u16, 0, 200) };
        try segments.append(segment);
        last_id += 1;
    }

    var total_merge_size: u64 = 0;
    var total_merge_count: u64 = 0;

    for (0..1) |i| {
        if (rand.boolean()) {
            const segment = MockSegment{ .id = last_id, .size = 100 + rand.intRangeAtMost(u16, 0, 200) };
            try segments.append(segment);
            last_id += 1;
        }

        const budget = policy.calculateBudget(segments.items);
        if (segments.items.len <= budget) {
            continue;
        }

        if (verbose) {
            std.debug.print("--- {} ---\n", .{i});
            std.debug.print("segments:\n", .{});
            var iter = segments.first;
            while (iter) |node| {
                std.debug.print("  {}: {}\n", .{ node.data.id, node.data.size });
                iter = node.next;
            }
        }

        const candidate = policy.findSegmentsToMerge(segments.items) orelse continue;

        total_merge_size += candidate.len;
        total_merge_count += 1;

        if (verbose) {
            std.debug.print("merging {}-{}\n", .{ candidate.start, candidate.end });
        }
        try applyMerge(&segments, candidate);
    }

    if (verbose) {
        std.debug.print("num merges: {}\n", .{total_merge_count});
        std.debug.print("avg merge size: {}\n", .{total_merge_size / total_merge_count});
    }
}
