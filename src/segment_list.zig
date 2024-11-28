const std = @import("std");

const common = @import("common.zig");
const SearchResults = common.SearchResults;

const SegmentMerger = @import("segment_merger.zig").SegmentMerger;
const TieredMergePolicy = @import("segment_merge_policy.zig").TieredMergePolicy;

const Deadline = @import("utils/Deadline.zig");

pub fn SegmentList(Segment: type) type {
    return struct {
        pub const Self = @This();
        pub const List = std.DoublyLinkedList(Segment);
        pub const Node = List.Node;

        pub const MergePolicy = TieredMergePolicy(Segment);

        allocator: std.mem.Allocator,
        merge_policy: MergePolicy,
        segments: List,

        num_allowed_segments: std.atomic.Value(usize),

        pub fn init(allocator: std.mem.Allocator, merge_policy: MergePolicy) Self {
            return .{
                .allocator = allocator,
                .merge_policy = merge_policy,
                .segments = .{},
                .num_allowed_segments = std.atomic.Value(usize).init(0),
            };
        }

        pub fn deinit(self: *Self) void {
            while (self.segments.popFirst()) |node| {
                self.destroySegment(node);
            }
        }

        // Creates a new segment and returns a pointer to the list node owning it.
        // This function is safe to call from any thread.
        pub fn createSegment(self: *Self) !*Node {
            const node = try self.allocator.create(Node);
            node.* = .{
                .data = Segment.init(self.allocator),
                .next = null,
                .prev = null,
            };
            return node;
        }

        // Destroys a segment and frees the memory.
        // This function is safe to call from any thread, but only if the segment is not in the list.
        pub fn destroySegment(self: *Self, node: *Node) void {
            node.data.deinit();
            self.allocator.destroy(node);
        }

        pub fn removeAndDestroySegment(self: *Self, node: *Node) void {
            self.segments.remove(node);
            self.destroySegment(node);
        }

        pub fn appendSegment(self: *Self, node: *Node) void {
            self.segments.append(node);
        }

        pub fn getIdsAfterAppend(self: *Self, new_segment: *Node, allocator: std.mem.Allocator) !std.ArrayList(common.SegmentId) {
            var ids = std.ArrayList(common.SegmentId).init(allocator);
            errdefer ids.deinit();

            try ids.ensureTotalCapacity(self.segments.len + 1);

            var it = self.segments.first;
            while (it) |node| : (it = node.next) {
                ids.appendAssumeCapacity(node.data.id);
            }

            ids.appendAssumeCapacity(new_segment.data.id);

            return ids;
        }

        pub fn getIdsAfterAppliedMerge(self: *Self, merge: PreparedMerge, allocator: std.mem.Allocator) !std.ArrayList(common.SegmentId) {
            var ids = std.ArrayList(common.SegmentId).init(allocator);
            errdefer ids.deinit();

            try ids.ensureTotalCapacity(self.segments.len - merge.sources.num_segments + 1);

            var it = self.segments.first;
            var inside_merge = false;
            while (it) |node| : (it = node.next) {
                if (it == merge.sources.start) {
                    inside_merge = true;
                }
                if (!inside_merge) {
                    ids.appendAssumeCapacity(node.data.id);
                }
                if (it == merge.sources.end) {
                    inside_merge = false;
                    ids.appendAssumeCapacity(merge.target.data.id);
                }
            }

            return ids;
        }

        pub fn getMaxCommitId(self: *const Self) u64 {
            var max_commit_id: u64 = 0;
            var it = self.segments.first;
            while (it) |node| : (it = node.next) {
                if (node.data.max_commit_id > max_commit_id) {
                    max_commit_id = node.data.max_commit_id;
                }
            }
            return max_commit_id;
        }

        pub fn hasNewerVersion(self: *const Self, doc_id: u32, version: u32) bool {
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
            defer results.removeOutdatedResults(self);
            var iter = self.segments.first;
            while (iter) |node| : (iter = node.next) {
                if (deadline.isExpired()) {
                    return error.Timeout;
                }
                try node.data.search(hashes, results);
            }
        }

        pub fn needsMerge(self: *Self) bool {
            return self.segments.len > self.num_allowed_segments.load(.monotonic);
        }

        pub const SegmentsToMerge = MergePolicy.Candidate;

        pub const PreparedMerge = struct {
            sources: SegmentsToMerge,
            target: *Node,
            merger: SegmentMerger(Segment),
        };

        pub fn prepareMerge(self: *Self) !?PreparedMerge {
            const result = self.merge_policy.findSegmentsToMerge(self.segments);
            self.num_allowed_segments.store(result.num_allowed_segments, .monotonic);

            const sources = result.candidate orelse return null;

            var merger = SegmentMerger(Segment).init(self.allocator, self);
            errdefer merger.deinit();

            var source_node = sources.start;
            while (true) {
                try merger.addSource(&source_node.data);
                if (source_node == sources.end) break;
                source_node = source_node.next orelse break;
            }
            try merger.prepare();

            const target = try self.createSegment();
            return .{
                .sources = sources,
                .merger = merger,
                .target = target,
            };
        }

        pub fn cleanupAfterMerge(self: *Self, merge: PreparedMerge, cleanup_args: anytype) void {
            var iter = merge.sources.start;
            while (true) {
                const next_node = iter.next;
                const is_end = iter == merge.sources.end;
                @call(.auto, Segment.cleanup, .{&iter.data} ++ cleanup_args);
                self.destroySegment(iter);
                if (is_end) break;
                iter = next_node orelse unreachable; // next_node being null implies a memory corruption
            }
        }

        pub fn applyMerge(self: *Self, merge: PreparedMerge) void {
            self.segments.insertBefore(merge.sources.start, merge.target);
            var iter = merge.sources.start;
            while (true) {
                const next_node = iter.next;
                const is_end = iter == merge.sources.end;
                self.segments.remove(iter);
                if (is_end) break;
                iter = next_node orelse unreachable; // next_node being null implies a memory corruption
            }
        }

        pub fn getTotalSize(self: Self) usize {
            var size: usize = 0;
            var iter = self.segments.first;
            while (iter) |node| : (iter = node.next) {
                size += node.data.getSize();
            }
            return size;
        }
    };
}

pub const SegmentMergeOptions = struct {
    max_segment_size: usize,

    pub fn getMaxSegments(self: SegmentMergeOptions, total_size: usize) usize {
        const max_level_size = @min(self.max_segment_size, @max(total_size / 2, 10));
        const min_level_size = @max(max_level_size / 1000, 10);
        const x = max_level_size / min_level_size;
        if (x == 0) {
            return 1;
        } else {
            return @max(1, std.math.log2_int(usize, x));
        }
    }
};

const MemorySegment = @import("MemorySegment.zig");
const FileSegment = @import("FileSegment.zig");
const SegmentId = @import("common.zig").SegmentId;

pub const AnySegment = union(enum) {
    file: FileSegment,
    memory: MemorySegment,

    pub fn search(self: AnySegment, sorted_hashes: []const u32, results: *SearchResults) !void {
        switch (self) {
            .file => |segment| try segment.search(sorted_hashes, results),
            .memory => |segment| try segment.search(sorted_hashes, results),
        }
    }

    pub fn getId(self: AnySegment) SegmentId {
        switch (self) {
            .file => |segment| return segment.id,
            .memory => |segment| return segment.id,
        }
    }

    pub fn getMaxCommitId(self: AnySegment) u64 {
        switch (self) {
            .file => |segment| return segment.max_commit_id,
            .memory => |segment| return segment.max_commit_id,
        }
    }
};

pub const AnySegmentNode = struct {
    segment: AnySegment,
    refs: std.atomic.Value(u32),
    next: std.atomic.Value(?*AnySegmentNode),

    pub fn create(comptime Segment: type, args: anytype, allocator: std.mem.Allocator) !*AnySegmentNode {
        const result = try allocator.create(AnySegmentNode);
        errdefer allocator.destroy(result);

        result.* = .{
            .segment = undefined,
            .refs = std.atomic.Value(u32).init(1),
            .next = std.atomic.Value(?*AnySegmentNode).init(null),
        };

        inline for (@typeInfo(AnySegment).Union.fields) |f| {
            if (f.type == Segment) {
                result.segment = @unionInit(AnySegment, f.name, @call(.auto, Segment.init, args ++ .{allocator}));
                break;
            }
        } else {
            @compileError("Unknown segment type '" ++ @typeName(Segment) ++ "'");
        }

        return result;
    }

    pub fn destroy(self: *AnySegmentNode, allocator: std.mem.Allocator) void {
        const refs = self.refs.load(.acquire);
        if (refs != 0) {
            std.debug.panic("trying to destroy segment with {} reference(s)", .{refs});
        }
        allocator.destroy(self);
    }

    pub fn search(self: *AnySegmentNode, sorted_hashes: []const u32, results: *SearchResults) !void {
        return self.segment.search(sorted_hashes, results);
    }

    pub fn getMaxCommitId(self: *AnySegmentNode) u64 {
        return self.segment.getMaxCommitId();
    }
};

pub const AnySegmentList = struct {
    allocator: std.mem.Allocator,
    head: std.atomic.Value(?*AnySegmentNode),
    replace_lock: std.Thread.Mutex = .{},

    pub fn init(allocator: std.mem.Allocator) AnySegmentList {
        return .{
            .allocator = allocator,
            .head = std.atomic.Value(?*AnySegmentNode).init(null),
        };
    }

    pub fn deinit(self: *AnySegmentList) void {
        var iter = self.head;
        while (iter.load(.acquire)) |node| {
            std.debug.print("deinit {}\n", .{node.segment.getId().version});
            const next_node = node.next;
            self.unref(node);
            iter = next_node;
        }
    }

    pub fn create(self: AnySegmentList, comptime Segment: type, args: anytype) !*AnySegmentNode {
        return try AnySegmentNode.create(Segment, args, self.allocator);
    }

    pub fn count(self: *AnySegmentList) usize {
        var result: usize = 0;
        var iter = self.head;
        while (iter.load(.acquire)) |node| : (iter = node.next) {
            result += 1;
        }
        return result;
    }

    pub fn search(self: *AnySegmentList, sorted_hashes: []const u32, results: *SearchResults, deadline: Deadline) !void {
        std.debug.print("search\n", .{});
        const head_node = self.head.load(.acquire) orelse return;
        self.ref(head_node);
        defer self.unref(head_node);

        var node = head_node;
        while (true) {
            if (deadline.isExpired()) {
                return error.Timeout;
            }
            try node.search(sorted_hashes, results);
            node = node.next.load(.acquire) orelse break;
        }
    }

    pub fn getMaxCommitId(self: *AnySegmentList) u64 {
        const head_node = self.head.load(.acquire) orelse return;
        self.ref(head_node);
        defer self.unref(head_node);

        var result: u64 = 0;
        var node = head_node;
        while (true) {
            result = @max(result, node.getMaxCommitId());
            node = node.next.load(.acquire) orelse break;
        }
        return result;
    }

    pub fn ref(self: AnySegmentList, node: *AnySegmentNode) void {
        // https://www.boost.org/doc/libs/1_55_0/doc/html/atomic/usage_examples.html#boost_atomic.usage_examples.example_reference_counters
        _ = self;
        const prev_ref_count = node.refs.fetchAdd(1, .monotonic);
        std.debug.assert(prev_ref_count > 0);
        std.debug.print("ref {} -> {}\n", .{ node.segment.getId().version, prev_ref_count + 1 });
    }

    pub fn unref(self: AnySegmentList, node: *AnySegmentNode) void {
        // https://www.boost.org/doc/libs/1_55_0/doc/html/atomic/usage_examples.html#boost_atomic.usage_examples.example_reference_counters
        const prev_ref_count = node.refs.fetchSub(1, .release);
        std.debug.print("unref {} -> {}\n", .{ node.segment.getId().version, prev_ref_count - 1 });
        if (prev_ref_count == 1) {
            node.refs.fence(.acquire);
            self.allocator.destroy(node);
        }
    }

    fn setNext(self: AnySegmentList, node: *AnySegmentNode, next: ?*AnySegmentNode) void {
        if (next) |next_node| {
            self.ref(next_node);
        }
        const prev_next = node.next.swap(next, .acq_rel);
        if (prev_next) |prev_next_node| {
            self.unref(prev_next_node);
        }
    }

    pub fn print(self: *AnySegmentList) void {
        var iter = self.head;
        while (iter.load(.acquire)) |node| : (iter = node.next) {
            std.debug.print("{}: refs={}\n", .{ node.segment.getId().version, node.refs.load(.acquire) });
        }
    }

    pub fn prepend(self: *AnySegmentList, node: *AnySegmentNode) void {
        // https://www.boost.org/doc/libs/1_55_0/doc/html/atomic/usage_examples.html#boost_atomic.usage_examples.mp_queue
        // https://en.cppreference.com/w/cpp/atomic/atomic/compare_exchange
        std.debug.print("prepend {}\n", .{node.segment.getId().version});
        self.ref(node);
        var stale_head = self.head.load(.monotonic);
        while (true) {
            self.setNext(node, stale_head);
            stale_head = self.head.cmpxchgWeak(stale_head, node, .release, .monotonic) orelse break;
        }
        if (stale_head) |prev_head| {
            self.unref(prev_head);
        }
    }

    pub fn replace(self: *AnySegmentList, new_node: *AnySegmentNode, old_node: *AnySegmentNode, old_count: usize) void {
        // This operation is safe regarding any read and prepend, but not other replaces.
        // We need to ensure that no other thread is trying to replace the overlapping set of nodes at the same time.
        // This is theoretically solvable in a non-blocking fashion, but it's too complex, and we are fine with blocking updates.
        // https://en.wikipedia.org/wiki/Non-blocking_linked_list
        std.debug.print("replace {}\n", .{new_node.segment.getId().version});

        std.debug.assert(old_count >= 1);

        self.replace_lock.lock();
        defer self.replace_lock.unlock();

        var prev: ?*AnySegmentNode = null;
        var next: ?*AnySegmentNode = null;

        {
            var iter = self.head;
            while (iter.load(.acquire)) |node| : (iter = node.next) {
                if (node == old_node) {
                    break;
                }
                prev = node;
            } else {
                std.debug.panic("old_node not found", .{});
            }
        }

        {
            next = prev;
            for (0..old_count + 1) |_| {
                if (next) |next_node| {
                    next = next_node.next.load(.acquire);
                } else {
                    break;
                }
            }
        }

        const prev_node = prev orelse std.debug.panic("can't replace head", .{});

        self.setNext(new_node, next);
        self.setNext(prev_node, new_node);
    }
};

test "AnySegment" {
    var list = AnySegmentList.init(std.testing.allocator);
    defer list.deinit();

    const node1 = try list.create(MemorySegment, .{});
    defer list.unref(node1);

    const node2 = try list.create(MemorySegment, .{});
    defer list.unref(node2);

    const node3 = try list.create(MemorySegment, .{});
    defer list.unref(node3);

    node1.segment.memory.id.version = 1;
    node2.segment.memory.id.version = 2;
    node3.segment.memory.id.version = 3;

    list.prepend(node2);
    list.prepend(node1);
    std.debug.print("before replace\n", .{});
    list.print();

    list.replace(node3, node2, 1);
    std.debug.print("after replace\n", .{});
    list.print();

    var results = SearchResults.init(std.testing.allocator);
    defer results.deinit();

    try list.search(&[_]u32{}, &results, .{});
}
