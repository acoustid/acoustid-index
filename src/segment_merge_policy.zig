const std = @import("std");
const log = std.log.scoped(.segment_merge_policy);

pub fn MergeCandidate(comptime T: type) type {
    return struct {
        start: *std.DoublyLinkedList(T).Node,
        end: *std.DoublyLinkedList(T).Node,
        num_segments: usize = 0,
        size: usize = 0,
        level_size: usize,
        level_no: usize,
    };
}

pub const TieredMergePolicy = struct {
    max_segment_size: usize,
    min_segment_size: usize,

    max_merge_size: u32 = 10,
    segments_per_level: u32 = 10,

    pub fn findMerges(self: TieredMergePolicy, comptime T: type, segments: std.DoublyLinkedList(T), allocator: std.mem.Allocator) !std.ArrayList(MergeCandidate(T)) {
        const Candidate = MergeCandidate(T);

        var candidates = std.ArrayList(Candidate).init(allocator);
        errdefer candidates.deinit();

        var total_size: usize = 0;
        var num_mergeable_segments: usize = 0;
        var min_segment_size: usize = std.math.maxInt(usize);

        {
            std.debug.print("segments:\n", .{});
            var iter = segments.first;
            while (iter) |node| : (iter = node.next) {
                const segment = &node.data;
                const size = segment.getSize();
                if (size > self.max_segment_size) {
                    std.debug.print("  segment {}: {} (too large)\n", .{ segment.id, size });
                    continue;
                }
                total_size += size;
                num_mergeable_segments += 1;
                min_segment_size = @min(min_segment_size, size);
                std.debug.print("  segment {}: {}\n", .{ segment.id, size });
            }
        }

        std.debug.print("total size: {}\n", .{total_size});
        std.debug.print("num mergeable segments: {}\n", .{num_mergeable_segments});

        var floor_level = self.min_segment_size;
        var top_level = floor_level;
        const merge_factor = @min(self.max_merge_size, self.segments_per_level);

        var allowed_segment_count: usize = 0;
        {
            var level_size = floor_level;
            var remaining_size = total_size;
            while (true) {
                if (level_size < self.min_segment_size) {
                    floor_level = level_size;
                } else {
                    const segments_per_level = remaining_size * 100 / level_size;
                    if (segments_per_level < self.segments_per_level * 100 or level_size >= self.max_segment_size) {
                        allowed_segment_count += segments_per_level;
                        top_level = level_size;
                        break;
                    }
                    allowed_segment_count += self.segments_per_level * 100;
                    remaining_size -= self.segments_per_level * level_size;
                }
                level_size = @min(self.max_segment_size, level_size * merge_factor);
            }
            allowed_segment_count = (allowed_segment_count + 50) / 100;
            std.debug.print("allowed segment count: {}\n", .{allowed_segment_count});
        }

        std.debug.print("floor level: {}\n", .{floor_level});
        std.debug.print("top level: {}\n", .{top_level});

        if (allowed_segment_count >= num_mergeable_segments) {
            return candidates;
        }

        {
            var level_size = floor_level;
            var level_boundary = level_size * merge_factor * 2 / 4;

            var level_no: usize = 0;
            var end_node = segments.last orelse return candidates;
            while (true) {
                if (end_node.data.getSize() > self.max_segment_size) {
                    end_node = end_node.prev orelse break;
                    continue;
                }

                const next_level_size = level_size * merge_factor;
                const next_level_boundary = next_level_size * merge_factor * 2 / 4;

                var start_node = end_node;
                while (true) {
                    if (start_node.prev) |prev_node| {
                        if (prev_node.data.getSize() <= level_boundary) {
                            start_node = prev_node;
                            continue;
                        }
                    }
                    break;
                }

                std.debug.print("level={} segments={}-{}\n", .{ level_size, start_node.data.id, end_node.data.id });

                var candidate = Candidate{
                    .start = start_node,
                    .end = start_node,
                    .num_segments = 0,
                    .size = 0,
                    .level_size = level_size,
                    .level_no = level_no,
                };

                var iter = start_node;
                while (true) {
                    if (candidate.num_segments >= self.max_merge_size or candidate.size >= self.max_segment_size or candidate.size >= level_boundary) {
                        break;
                    }
                    candidate.end = iter;
                    candidate.num_segments += 1;
                    candidate.size += iter.data.getSize();
                    if (iter == end_node) break;
                    iter = iter.next orelse break;
                }

                if (candidate.num_segments > 1) {
                    if (candidate.size >= level_boundary or candidate.size < next_level_boundary or true) {
                        const prev_size: usize = if (candidate.start.prev) |prev_node| prev_node.data.getSize() else std.math.maxInt(usize);
                        if (prev_size > candidate.size * 75 / 100) {
                            try candidates.append(candidate);
                        } else {
                            std.debug.print("skipping candidate {}-{}, because size={} and prev_size={}\n", .{ candidate.start.data.id, candidate.end.data.id, candidate.size, prev_size });
                        }
                    }
                }

                level_size = next_level_size;
                level_boundary = next_level_boundary;
                level_no += 1;
                end_node = start_node.prev orelse break;
            }
        }

        std.debug.print("candidate:\n", .{});
        for (candidates.items) |c| {
            std.debug.print("  {}-{}: {} {} level_size={}\n", .{ c.start.data.id, c.end.data.id, c.size, c.num_segments, c.level_size });
        }
        return candidates;
    }
};

const MockSegment = struct {
    id: u64,
    size: usize,

    pub fn getSize(self: @This()) usize {
        return self.size;
    }
};

const MockSegmentList = std.DoublyLinkedList(MockSegment);

fn applyMerge(comptime T: type, segments: *std.DoublyLinkedList(T), merge: MergeCandidate(T), allocator: std.mem.Allocator) !void {
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

test {
    var segments: std.DoublyLinkedList(MockSegment) = .{};

    defer {
        var iter = segments.first;
        while (iter) |node| {
            iter = node.next;
            std.testing.allocator.destroy(node);
        }
    }

    var last_id: u64 = 1;

    var prng = std.rand.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        try std.posix.getrandom(std.mem.asBytes(&seed));
        break :blk seed;
    });
    const rand = prng.random();

    const policy = TieredMergePolicy{
        .min_segment_size = 100,
        .max_segment_size = 100000,
        .max_merge_size = 3,
        .segments_per_level = 3,
    };

    for (0..10) |_| {
        var segment = try std.testing.allocator.create(MockSegmentList.Node);
        segment.data = .{ .id = last_id, .size = 100 + rand.intRangeAtMost(u8, 0, 50) };
        segments.append(segment);
        last_id += 1;
    }

    for (0..1) |_| {
        if (rand.boolean()) {
            var segment = try std.testing.allocator.create(MockSegmentList.Node);
            segment.data = .{ .id = last_id, .size = 100 + rand.intRangeAtMost(u8, 0, 50) };
            segments.append(segment);
            last_id += 1;
        }

        var candidates = try policy.findMerges(MockSegment, segments, std.testing.allocator);
        defer candidates.deinit();

        if (candidates.items.len > 0) {
            try applyMerge(MockSegment, &segments, candidates.items[0], std.testing.allocator);
        }
        std.debug.print("---\n", .{});
    }
}
