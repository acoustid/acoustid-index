const std = @import("std");

pub const LogMergePolicy = struct {
    pub const default_merge_factor = 10;

    // How many segments to merge at a time.
    merge_factor: usize = default_merge_factor,

    // Any segments whose size is smaller than this value
    // will be rounded up to this value.  This ensures that
    // tiny segments are aggressively merged.
    min_merge_size: usize,

    // If the size of a segment exceeds this value then it
    // will never be merged.
    max_merge_size: usize,

    // If true, we pro-rate a segment's size by the
    // percentage of non-deleted documents.
    calibrate_size_by_deletes: bool = false,

    pub fn findMerges(self: *LogMergePolicy, comptime T: type, segments: std.DoublyLinkedList(T)) !void {
        _ = self;
        _ = segments;
    }
};
