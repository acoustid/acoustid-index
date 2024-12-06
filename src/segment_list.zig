const std = @import("std");
const Allocator = std.mem.Allocator;

const SearchResults = @import("common.zig").SearchResults;

const Change = @import("change.zig").Change;
const KeepOrDelete = @import("common.zig").KeepOrDelete;
const SegmentInfo = @import("segment.zig").SegmentInfo;
const DocInfo = @import("common.zig").DocInfo;

const Deadline = @import("utils/Deadline.zig");

const SharedPtr = @import("utils/shared_ptr.zig").SharedPtr;
const TieredMergePolicy = @import("segment_merge_policy.zig").TieredMergePolicy;
const SegmentMerger = @import("segment_merger.zig").SegmentMerger;

pub fn SegmentList(Segment: type) type {
    return struct {
        pub const Self = @This();

        pub const Node = SharedPtr(Segment);
        pub const List = std.ArrayListUnmanaged(Node);

        nodes: List,

        pub fn initEmpty() Self {
            const nodes = List.initBuffer(&.{});
            return .{
                .nodes = nodes,
            };
        }

        pub fn init(allocator: Allocator, num: usize) Allocator.Error!Self {
            const nodes = try List.initCapacity(allocator, num);
            errdefer nodes.deinit(allocator);

            return .{
                .nodes = nodes,
            };
        }

        pub fn createSharedEmpty(allocator: Allocator) Allocator.Error!SharedPtr(Self) {
            return try SharedPtr(Self).create(allocator, Self.initEmpty());
        }

        pub fn createShared(allocator: Allocator, capacity: anytype) Allocator.Error!SharedPtr(Self) {
            var self = try Self.init(allocator, capacity);
            errdefer self.deinit(allocator);

            return try SharedPtr(Self).create(allocator, self);
        }

        pub fn deinit(self: *Self, allocator: Allocator, delete_files: KeepOrDelete) void {
            for (self.nodes.items) |*node| {
                node.release(allocator, Segment.deinit, .{delete_files});
            }
            self.nodes.deinit(allocator);
        }

        pub fn createSegment(allocator: Allocator, options: Segment.Options) Allocator.Error!Node {
            return Node.create(allocator, @call(.auto, Segment.init, .{ allocator, options }));
        }

        pub fn destroySegment(allocator: Allocator, segment: *Node) void {
            segment.release(allocator, Segment.deinit, .{.delete});
        }

        pub fn loadSegment(allocator: Allocator, info: SegmentInfo, options: Segment.Options) !Node {
            var node = try createSegment(allocator, options);
            errdefer destroySegment(allocator, &node);

            try node.value.load(info);

            return node;
        }

        pub fn destroySegments(allocator: Allocator, segments: *SharedPtr(Self)) void {
            segments.release(allocator, Self.deinit, .{ allocator, .delete });
        }

        pub fn appendSegmentInto(self: Self, copy: *Self, node: Node) void {
            copy.nodes.clearRetainingCapacity();
            for (self.nodes.items) |n| {
                copy.nodes.appendAssumeCapacity(n.acquire());
            }
            copy.nodes.appendAssumeCapacity(node.acquire());
        }

        pub fn removeSegmentInto(self: Self, copy: *Self, node: Node) void {
            copy.nodes.clearRetainingCapacity();
            for (self.nodes.items) |n| {
                if (n.value != node.value) {
                    copy.nodes.appendAssumeCapacity(n.acquire());
                }
            }
        }

        pub fn replaceMergedSegmentInto(self: *Self, copy: *Self, node: Node) void {
            copy.nodes.clearRetainingCapacity();
            var inserted_merged = false;
            for (self.nodes.items) |n| {
                if (node.value.info.contains(n.value.info)) {
                    if (!inserted_merged) {
                        copy.nodes.appendAssumeCapacity(node.acquire());
                        inserted_merged = true;
                    }
                } else {
                    copy.nodes.appendAssumeCapacity(n.acquire());
                }
            }
        }

        pub fn getMaxDocId(self: Self) u32 {
            var result: u32 = 0;
            for (self.nodes.items) |node| {
                result = @max(result, node.value.max_doc_id);
            }
            return result;
        }

        pub fn getDocInfo(self: Self, doc_id: u32) ?DocInfo {
            var result: ?DocInfo = null;
            for (self.nodes.items) |node| {
                const active = node.value.docs.get(doc_id) orelse continue;
                result = .{ .version = node.value.info.version, .deleted = !active };
            }
            return result;
        }

        pub fn search(self: Self, hashes: []const u32, results: *SearchResults, deadline: Deadline) !void {
            for (self.nodes.items) |node| {
                if (deadline.isExpired()) {
                    return error.Timeout;
                }
                try node.value.search(hashes, results);
            }
            results.removeOutdatedResults(self);
        }

        fn compareByVersion(_: void, lhs: u32, rhs: Node) bool {
            return lhs < rhs.value.id.version;
        }

        pub fn hasNewerVersion(self: *const Self, doc_id: u32, version: u64) bool {
            var i = self.nodes.items.len;
            while (i > 0) {
                i -= 1;
                const node = self.nodes.items[i];
                if (node.value.info.version > version) {
                    if (node.value.docs.contains(doc_id)) {
                        return true;
                    }
                } else {
                    break;
                }
            }
            return false;
        }

        pub fn count(self: Self) usize {
            return self.nodes.items.len;
        }

        pub fn getFirst(self: Self) ?Node {
            return if (self.nodes.items.len > 0) self.nodes.items[0] else null;
        }

        pub fn getLast(self: Self) ?Node {
            return self.nodes.getLastOrNull();
        }
    };
}

fn getSegmentSize(comptime T: type) fn (SharedPtr(T)) usize {
    const tmp = struct {
        fn getSize(segment: SharedPtr(T)) usize {
            return segment.value.getSize();
        }
    };
    return tmp.getSize;
}

pub fn SegmentListManager(Segment: type) type {
    return struct {
        pub const Self = @This();
        pub const List = SegmentList(Segment);
        pub const MergePolicy = TieredMergePolicy(List.Node, getSegmentSize(Segment));

        options: Segment.Options,
        segments: SharedPtr(List),
        merge_policy: MergePolicy,
        num_allowed_segments: std.atomic.Value(usize),
        update_lock: std.Thread.Mutex,

        pub fn init(allocator: Allocator, options: Segment.Options, merge_policy: MergePolicy) !Self {
            const segments = try SharedPtr(List).create(allocator, List.initEmpty());
            return Self{
                .options = options,
                .segments = segments,
                .merge_policy = merge_policy,
                .num_allowed_segments = std.atomic.Value(usize).init(0),
                .update_lock = .{},
            };
        }

        pub fn deinit(self: *Self, allocator: Allocator, delete_files: KeepOrDelete) void {
            self.segments.release(allocator, List.deinit, .{ allocator, delete_files });
        }

        pub fn count(self: Self) usize {
            return self.segments.value.nodes.items.len;
        }

        fn acquireSegments(self: *Self) SharedPtr(List) {
            self.update_lock.lock();
            defer self.update_lock.unlock();

            return self.segments.acquire();
        }

        fn destroySegments(allocator: Allocator, segments: *SharedPtr(List)) void {
            List.destroySegments(allocator, segments);
        }

        pub fn needsMerge(self: Self) bool {
            return self.segments.value.nodes.items.len > self.num_allowed_segments.load(.acquire);
        }

        pub fn prepareMerge(self: *Self, allocator: Allocator) !?Update {
            var segments = self.acquireSegments();
            defer destroySegments(allocator, &segments);

            self.num_allowed_segments.store(self.merge_policy.calculateBudget(segments.value.nodes.items), .release);
            if (!self.needsMerge()) {
                return null;
            }

            const candidate = self.merge_policy.findSegmentsToMerge(segments.value.nodes.items) orelse return null;

            var target = try List.createSegment(allocator, self.options);
            defer List.destroySegment(allocator, &target);

            var merger = try SegmentMerger(Segment).init(allocator, segments.value, candidate.end - candidate.start);
            defer merger.deinit();

            for (segments.value.nodes.items[candidate.start..candidate.end]) |segment| {
                merger.addSource(segment.value);
            }
            try merger.prepare();

            try target.value.merge(&merger);
            errdefer target.value.cleanup();

            var update = try self.beginUpdate(allocator);
            update.replaceMergedSegment(target);

            return update;
        }

        pub const Update = struct {
            manager: *Self,
            segments: SharedPtr(List),
            committed: bool = false,

            pub fn removeSegment(self: *@This(), node: List.Node) void {
                self.manager.segments.value.removeSegmentInto(self.segments.value, node);
            }

            pub fn appendSegment(self: *@This(), node: List.Node) void {
                self.manager.segments.value.appendSegmentInto(self.segments.value, node);
            }

            pub fn replaceMergedSegment(self: *@This(), node: List.Node) void {
                self.manager.segments.value.replaceMergedSegmentInto(self.segments.value, node);
            }
        };

        pub fn beginUpdate(self: *Self, allocator: Allocator) !Update {
            self.update_lock.lock();
            errdefer self.update_lock.unlock();

            var segments = try SharedPtr(List).create(allocator, List.initEmpty());
            errdefer destroySegments(allocator, &segments);

            // allocate memory for one extra segment, if it's going to be unused, it's going to be unused, but we need to have it ready
            try segments.value.nodes.ensureTotalCapacity(allocator, self.count() + 1);

            return .{
                .manager = self,
                .segments = segments,
            };
        }

        pub fn commitUpdate(self: *Self, update: *Update) void {
            self.segments.swap(&update.segments);
            self.update_lock.unlock();
            update.committed = true;
        }

        pub fn cleanupAfterUpdate(self: *Self, allocator: Allocator, update: *Update) void {
            if (!update.committed) {
                self.update_lock.unlock();
            }
            destroySegments(allocator, &update.segments);
        }
    };
}
