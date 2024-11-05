const std = @import("std");
const log = std.log.scoped(.segment);
const assert = std.debug.assert;

const common = @import("common.zig");
const Item = common.Item;
const SearchResults = common.SearchResults;
const SegmentID = common.SegmentID;

const InMemorySegment = @import("InMemorySegment.zig");

const filefmt = @import("filefmt.zig");

const segment_list = @import("segment_list.zig");
pub const List = segment_list.SegmentList(Self);

const Self = @This();

allocator: std.mem.Allocator,
id: SegmentID = .{ .version = 0, .included_merges = 0 },
max_commit_id: u64 = 0,
docs: std.AutoHashMap(u32, bool),
index: std.ArrayList(u32),
block_size: usize = 0,
blocks: []const u8,
merged: u32 = 0,
num_items: usize = 0,

raw_data: ?[]align(std.mem.page_size) u8 = null,

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
                try results.incr(block_items.items[i].id, self.id.version);
            }
        }
    }
}

fn read(self: *Self, dir: std.fs.Dir, file_name: []const u8) !void {
    const file = try dir.openFile(file_name, .{});
    defer file.close();
    try filefmt.readFile(file, self);
}

pub fn open(self: *Self, dir: std.fs.Dir, id: SegmentID) !void {
    var file_name_buf: [filefmt.max_file_name_size]u8 = undefined;
    const file_name = filefmt.buildSegmentFileName(&file_name_buf, id);

    std.debug.print("Reading segment {s}\n", .{file_name});

    try self.read(dir, file_name);
}

pub fn delete(self: *Self, dir: std.fs.Dir) void {
    var file_name_buf: [filefmt.max_file_name_size]u8 = undefined;
    const file_name = filefmt.buildSegmentFileName(&file_name_buf, self.id);

    dir.deleteFile(file_name) catch |err| {
        log.err("failed to clean up segment file {s}: {}", .{ file_name, err });
    };
}

pub fn convert(self: *Self, dir: std.fs.Dir, source: *InMemorySegment) !void {
    if (!source.frozen) {
        return error.SourceSegmentNotFrozen;
    }
    if (source.id.version == 0) {
        return error.SourceSegmentNoVersion;
    }
    if (source.id.included_merges != 0) {
        return error.SourceSegmentHasMerges;
    }

    var file_name_buf: [filefmt.max_file_name_size]u8 = undefined;
    const file_name = filefmt.buildSegmentFileName(&file_name_buf, source.id);

    var file = try dir.atomicFile(file_name, .{});
    defer file.deinit();

    try filefmt.writeFile(file.file, source);
    try file.finish();

    errdefer dir.deleteFile(file_name) catch |err| {
        log.err("failed to clean up segment file {s}: {}", .{ file_name, err });
    };

    try self.read(dir, file_name);
}

test "convert" {
    var tmpDir = std.testing.tmpDir(.{});
    defer tmpDir.cleanup();

    var source = InMemorySegment.init(std.testing.allocator);
    defer source.deinit();

    source.id.version = 1;
    source.frozen = true;
    try source.docs.put(1, true);
    try source.items.append(.{ .id = 1, .hash = 1 });
    try source.items.append(.{ .id = 1, .hash = 2 });

    var segment = Self.init(std.testing.allocator);
    defer segment.deinit();

    try segment.convert(tmpDir.dir, &source);

    try std.testing.expectEqual(1, segment.id.version);
    try std.testing.expectEqual(0, segment.id.included_merges);
    try std.testing.expectEqual(1, segment.docs.count());
    try std.testing.expectEqual(1, segment.index.items.len);
}

pub fn merge(self: *Self, dir: std.fs.Dir, segments_to_merge: List.SegmentsToMerge, collection: List) !void {
    const version = SegmentID.merge(segments_to_merge.node1.data.id, segments_to_merge.node2.data.id);

    var file_name_buf: [filefmt.max_file_name_size]u8 = undefined;
    const file_name = filefmt.buildSegmentFileName(&file_name_buf, version);

    var file = try dir.atomicFile(file_name, .{});
    defer file.deinit();

    try filefmt.mergeAndWriteFile(file.file, segments_to_merge, collection, self.allocator);
    try file.finish();

    errdefer dir.deleteFile(file_name) catch |err| {
        log.err("failed to clean up segment file {s}: {}", .{ file_name, err });
    };

    try self.read(dir, file_name);
}

pub fn canBeMerged(self: Self) bool {
    _ = self;
    return true;
}

pub fn getSize(self: Self) usize {
    return self.num_items;
}

pub fn reader(self: *Self) Reader {
    return Reader.init(self);
}

pub const Reader = struct {
    segment: *Self,
    block_no: usize,

    pub fn init(segment: *Self) Reader {
        return Reader{
            .segment = segment,
            .block_no = 0,
        };
    }

    pub fn read(self: *Reader, items: *std.ArrayList(Item)) !bool {
        if (self.block_no >= self.segment.index.items.len) {
            return false;
        }
        const block_data = self.segment.getBlockData(self.block_no);
        try filefmt.readBlock(block_data, items);
        self.block_no += 1;
        return true;
    }
};
