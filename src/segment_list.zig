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
    };
}
