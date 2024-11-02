const std = @import("std");
const assert = std.debug.assert;

const common = @import("common.zig");
const Item = common.Item;
const SearchResults = common.SearchResults;

const InMemorySegment = @import("InMemorySegment.zig");

const filefmt = @import("filefmt.zig");

pub const Version = std.meta.Tuple(&.{ u32, u32 });

allocator: std.mem.Allocator,
version: Version = .{ 0, 0 },
max_commit_id: u64 = 0,
docs: std.AutoHashMap(u32, bool),
index: std.ArrayList(u32),
block_size: usize = 0,
blocks: []const u8,
merged: u32 = 0,

raw_data: ?[]align(std.mem.page_size) u8 = null,

const Self = @This();

pub fn init(allocator: std.mem.Allocator) Self {
    return Self{
        .allocator = allocator,
        .docs = std.AutoHashMap(u32, bool).init(allocator),
        .index = std.ArrayList(u32).init(allocator),
        .blocks = undefined,
    };
}

pub fn deinit(self: *Self) void {
    self.docs.deinit();
    self.index.deinit();

    if (self.raw_data) |data| {
        std.posix.munmap(data);
        self.raw_data = null;
    }
}

pub fn getBlockData(self: *Self, block: usize) []const u8 {
    return self.blocks[block * self.block_size .. (block + 1) * self.block_size];
}

pub fn search(self: *Self, hashes: []const u32, results: *SearchResults) !void {
    assert(std.sort.isSorted(u32, hashes, {}, std.sort.asc(u32)));

    var prev_block_no: usize = std.math.maxInt(usize);
    var prev_block_range_start: usize = 0;

    var block_items = std.ArrayList(Item).init(self.allocator);
    defer block_items.deinit();

    for (hashes) |hash| {
        var block_no = std.sort.lowerBound(u32, hash, self.index.items, {}, std.sort.asc(u32));
        if (block_no == self.index.items.len) {
            block_no = prev_block_range_start;
        }
        prev_block_range_start = block_no;

        while (block_no < self.index.items.len and self.index.items[block_no] <= hash) : (block_no += 1) {
            if (block_no != prev_block_no) {
                prev_block_no = block_no;
                const block_data = self.getBlockData(block_no);
                try filefmt.readBlock(block_data, &block_items);
            }
            const matches = std.sort.equalRange(Item, Item{ .hash = hash, .id = 0 }, block_items.items, {}, Item.cmpByHash);
            for (matches[0]..matches[1]) |i| {
                try results.incr(block_items.items[i].id, self.version[1]);
            }
        }
    }
}

const max_file_name_size = 255;
const file_name_fmt = "segment-{d}-{d}.dat";

fn read(self: *Self, dir: std.fs.Dir, file_name: []const u8) !void {
    const file = try dir.openFile(file_name, .{});
    defer file.close();
    try filefmt.readFile(file, self);
}

pub fn open(self: *Self, dir: std.fs.Dir, version: Version) !void {
    var file_name_buf: [max_file_name_size]u8 = undefined;
    const file_name = try std.fmt.bufPrint(&file_name_buf, file_name_fmt, .{ version[0], version[1] });

    std.debug.print("Reading segment {s}\n", .{file_name});

    try self.read(dir, file_name);
}

pub fn delete(self: *Self, dir: std.fs.Dir) !void {
    var file_name_buf: [max_file_name_size]u8 = undefined;
    const file_name = try std.fmt.bufPrint(&file_name_buf, file_name_fmt, .{ self.version[0], self.version[1] });

    try dir.deleteFile(file_name);
}

pub fn convert(self: *Self, dir: std.fs.Dir, source: *InMemorySegment) !void {
    if (!source.frozen) {
        return error.SourceSegmentNotFrozen;
    }
    if (source.version == 0) {
        return error.SourceSegmentNoVersion;
    }

    var file_name_buf: [max_file_name_size]u8 = undefined;
    const file_name = try std.fmt.bufPrint(&file_name_buf, file_name_fmt, .{ source.version, source.version });

    var file = try dir.atomicFile(file_name, .{});
    defer file.deinit();
    try filefmt.writeFile(file.file, source);
    try file.finish();

    try self.read(dir, file_name);
}

test "convert" {
    var tmpDir = std.testing.tmpDir(.{});
    defer tmpDir.cleanup();

    var source = InMemorySegment.init(std.testing.allocator);
    defer source.deinit();

    source.version = 1;
    source.frozen = true;
    try source.docs.put(1, true);
    try source.items.append(.{ .id = 1, .hash = 1 });
    try source.items.append(.{ .id = 1, .hash = 2 });

    var segment = Self.init(std.testing.allocator);
    defer segment.deinit();

    try segment.convert(tmpDir.dir, &source);

    try std.testing.expectEqual(1, segment.version[0]);
    try std.testing.expectEqual(1, segment.version[1]);
    try std.testing.expectEqual(1, segment.docs.count());
    try std.testing.expectEqual(1, segment.index.items.len);
}

pub fn merge(self: *Self, dir: std.fs.Dir, sources: [2]*Self) !void {
    if (sources[0].version[1] + 1 != sources[1].version[0]) {
        return error.SourceSegmentVersionMismatch;
    }

    self.version[0] = sources[0].version[0];
    self.version[1] = sources[1].version[1];

    self.max_commit_id = @max(sources[0].max_commit_id, sources[1].max_commit_id);

    var file_name_buf: [max_file_name_size]u8 = undefined;
    const file_name = try std.fmt.bufPrint(&file_name_buf, file_name_fmt, .{ self.version[0], self.version[1] });

    var file = try dir.atomicFile(file_name, .{});
    defer file.deinit();

    try filefmt.writeFileFromTwoSegments(file.file, sources);
    try file.finish();

    try self.read(dir, file_name);
}
