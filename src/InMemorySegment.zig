const std = @import("std");
const log = std.log;
const io = std.io;

const Item = @import("common.zig").Item;

const filefmt = @import("filefmt.zig");

const Self = @This();

allocator: std.mem.Allocator,
version: u32,
docs: std.AutoHashMap(u32, bool),
items: std.ArrayList(Item),
frozen: bool = false,
merged: u32 = 0,

pub fn init(allocator: std.mem.Allocator) Self {
    return .{
        .allocator = allocator,
        .docs = std.AutoHashMap(u32, bool).init(allocator),
        .items = std.ArrayList(Item).init(allocator),
        .version = 0,
    };
}

pub fn deinit(self: *Self) void {
    self.docs.deinit();
    self.items.deinit();
}

pub fn writeToFile(self: *Self, writer: io.Writer, blockSize: comptime_int) !void {
    var blockData: [blockSize]u8 = undefined;

    while (true) {
        const written = try filefmt.writeBlock(blockData, self.items.items);
        _ = written;
        try writer.writeAll(blockData);
    }
}
