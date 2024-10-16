const std = @import("std");

const Segment = @import("Segment.zig");

const Self = @This();

const Segments = std.DoublyLinkedList(*Segment);

allocator: std.mem.Allocator,
segments: Segments,

pub fn init(allocator: std.mem.Allocator) Self {
    return Self{
        .allocator = allocator,
        .segments = .{},
    };
}

pub fn deinit(self: *Self) void {
    while (self.segments.popFirst()) |node| {
        self.destroyNode(node);
    }
}

pub fn createSegment(self: *Self) !*Segment {
    const segment = try self.allocator.create(Segment);
    segment.* = Segment.init(self.allocator);
    return segment;
}

fn destroySegment(self: *Self, segment: *Segment) void {
    segment.deinit();
    self.allocator.destroy(segment);
}

fn destroyNode(self: *Self, node: *Segments.Node) void {
    self.destroySegment(node.data);
    self.allocator.destroy(node);
}

pub fn append(self: *Self, segment: *Segment) !void {
    const node = try self.allocator.create(Segments.Node);
    node.data = segment;
    self.segments.append(node);
}
