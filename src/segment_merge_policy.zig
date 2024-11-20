const std = @import("std");
const log = std.log.scoped(.segment_merge_policy);

pub fn TieredMergePolicy(comptime T: type) type {
    return struct {
        max_segment_size: usize,
        min_segment_size: usize,

        max_merge_size: u32 = 10,
        segments_per_level: u32 = 10,

        const SegmentList = std.DoublyLinkedList(T);
        const SegmentNode = SegmentList.Node;

        const Candidate = struct {
            start: *SegmentNode,
            end: *SegmentNode,
            num_segments: usize = 0,
            size: usize = 0,
            level_size: usize,
            level_no: usize,
        };

        pub fn calculateBudget(self: TieredMergePolicy, segments: SegmentList) usize {
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
            const merge_factor = @min(self.max_merge_size, self.segments_per_level);

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

        pub fn findMerges(self: TieredMergePolicy, segments: std.DoublyLinkedList(T), allocator: std.mem.Allocator) !std.ArrayList(Candidate) {
            const num_allowed_segments = self.calculateBudget(segments);
            log.debug("budget: {} segments", .{num_allowed_segments});

            var candidates = std.ArrayList(Candidate).init(allocator);
            errdefer candidates.deinit();

            if (num_allowed_segments >= segments.len) {
                return candidates;
            }

            const merge_factor = @min(self.max_merge_size, self.segments_per_level);
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
