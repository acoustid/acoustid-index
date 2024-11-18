const std = @import("std");
const log = std.log.scoped(.segment_merge_policy);

pub const LogMergePolicy = struct {
    // Defines the allowed range of log(size) for each
    // level. A level is computed by taking the max segment
    // log size, minus level_log_span, and finding all
    // segments falling within that range.
    const level_log_span: f64 = 0.75;

    // Default merge factor, which is how many segments are
    // merged at a time.
    pub const default_merge_factor: usize = 10;

    // How many segments to merge at a time.
    merge_factor: usize = default_merge_factor,

    // Any segments whose size is smaller than this value
    // will be rounded up to this value. This ensures that
    // tiny segments are aggressively merged.
    min_merge_size: usize,

    // If the size of a segment exceeds this value then it
    // will never be merged.
    max_merge_size: usize,

    // If true, we pro-rate a segment's size by the
    // percentage of non-deleted documents.
    calibrate_size_by_deletes: bool = false,

    // Log details about the segment selection process.
    verbose: bool = false,

    fn logSize(size: usize) f64 {
        return @log2(@as(f64, @floatFromInt(@max(1, size))));
    }

    pub fn findMerges(self: LogMergePolicy, comptime T: type, segments: std.DoublyLinkedList(T), allocator: std.mem.Allocator) !void {
        const LevelAndSegment = struct {
            level: f64,
            segment: *const T,
            merging: bool = false,
        };

        var levels = std.ArrayList(LevelAndSegment).init(allocator);
        defer levels.deinit();

        const norm = logSize(self.merge_factor);

        std.debug.print("candidates:\n", .{});
        var iter = segments.first;
        while (iter) |node| : (iter = node.next) {
            const segment = &node.data;
            const level = logSize(segment.getSize()) / norm;
            std.debug.print("  segment {}: {}\n", .{ segment.id, segment.getSize() });
            try levels.append(.{ .level = level, .segment = segment });
        }

        const level_floor = logSize(self.min_merge_size) / norm;
        std.debug.print("level_floor: {}\n", .{level_floor});

        const num_mergeable_segments = levels.items.len;

        var start: usize = 0;
        while (start < num_mergeable_segments) {

            // Find max level of all segments not already
            // quantized.
            var max_level: f64 = 0.0;
            for (levels.items[start..]) |item| {
                max_level = @max(max_level, item.level);
            }

            // Now search backwards for the rightmost segment that
            // falls into this level:
            var level_bottom: f64 = undefined;
            if (max_level <= level_floor) {
                // All remaining segments fall into the min level
                level_bottom = -1.0;
            } else {
                level_bottom = max_level - level_log_span;

                // Force a boundary at the level floor
                if (level_bottom < level_floor and max_level >= level_floor) {
                    level_bottom = level_floor;
                }
            }

            std.debug.print("level_bottom: {}\n", .{level_bottom});

            var upto: usize = num_mergeable_segments - 1;
            while (upto >= start) {
                if (levels.items[upto].level >= level_bottom) {
                    break;
                }
                if (upto > start) {
                    upto -= 1;
                } else {
                    break;
                }
            }

            // Finally, record all merges that are viable at this level:
            var end: usize = start + self.merge_factor;
            while (end <= 1 + upto) {
                var any_too_large = false;
                var any_merging = false;

                for (levels.items[start..end]) |item| {
                    if (item.segment.getSize() > self.max_merge_size) {
                        any_too_large = true;
                    }
                    if (item.merging) {
                        any_merging = true;
                        break;
                    }
                }

                if (!any_too_large and !any_merging) {
                    std.debug.print("merge:\n", .{});
                    for (levels.items[start..end]) |*item| {
                        std.debug.print("  segment {}: {}\n", .{ item.segment.id, item.segment.getSize() });
                        item.merging = true;
                    }
                }

                start = end;
                end = start + self.merge_factor;
            }

            start = 1 + upto;
        }
    }
};

test {
    const MockSegment = struct {
        id: u64,
        size: usize,

        pub fn getSize(self: @This()) usize {
            return self.size;
        }
    };

    const MockSegmentList = std.DoublyLinkedList(MockSegment);

    var segments: std.DoublyLinkedList(MockSegment) = .{};

    defer {
        var iter = segments.first;
        while (iter) |node| {
            iter = node.next;
            std.testing.allocator.destroy(node);
        }
    }

    for (0..10) |i| {
        var segment = try std.testing.allocator.create(MockSegmentList.Node);
        segment.data = .{ .id = i, .size = 100 + (10 - i) * 10 };
        segments.append(segment);
    }

    const policy = LogMergePolicy{
        .min_merge_size = 1,
        .max_merge_size = 1000,
        .merge_factor = 3,
    };

    try policy.findMerges(MockSegment, segments, std.testing.allocator);
}
