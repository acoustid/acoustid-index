const std = @import("std");
const log = std.log.scoped(.segment);
const assert = std.debug.assert;

const common = @import("common.zig");
const SearchResults = common.SearchResults;
const KeepOrDelete = common.KeepOrDelete;
const Deadline = @import("utils/Deadline.zig");

const Item = @import("segment.zig").Item;
const SegmentInfo = @import("segment.zig").SegmentInfo;
const SegmentStatus = @import("segment.zig").SegmentStatus;
const metrics = @import("metrics.zig");

const filefmt = @import("filefmt.zig");

const Self = @This();

pub const Options = struct {
    dir: std.fs.Dir,
};

allocator: std.mem.Allocator,
dir: std.fs.Dir,
info: SegmentInfo = .{},
status: SegmentStatus = .{},
attributes: std.StringHashMapUnmanaged(u64) = .{},
docs: std.AutoHashMapUnmanaged(u32, bool) = .{},
min_doc_id: u32 = 0,
max_doc_id: u32 = 0,
index: std.ArrayListUnmanaged(u32) = .{},
block_size: usize = 0,
blocks: []const u8,
merged: u32 = 0,
num_items: usize = 0,
delete_in_deinit: bool = false,

mmaped_file: ?std.fs.File = null,
mmaped_data: ?[]align(std.heap.page_size_min) u8 = null,

pub fn init(allocator: std.mem.Allocator, options: Options) Self {
    return Self{
        .allocator = allocator,
        .dir = options.dir,
        .blocks = undefined,
    };
}

pub fn deinit(self: *Self, delete_file: KeepOrDelete) void {
    var iter = self.attributes.iterator();
    while (iter.next()) |e| {
        self.allocator.free(e.key_ptr.*);
    }
    self.attributes.deinit(self.allocator);
    self.docs.deinit(self.allocator);
    self.index.deinit(self.allocator);

    if (self.mmaped_data) |data| {
        std.posix.munmap(data);
    }

    if (self.mmaped_file) |file| {
        file.close();
    }

    if (delete_file == .delete) {
        self.delete();
    }
}

pub fn getBlockData(self: Self, block: usize) []const u8 {
    return self.blocks[block * self.block_size .. (block + 1) * self.block_size];
}

fn compareHashes(a: u32, b: u32) std.math.Order {
    return std.math.order(a, b);
}

pub fn search(self: Self, sorted_hashes: []const u32, results: *SearchResults, deadline: Deadline) !void {
    var prev_block_no: usize = std.math.maxInt(usize);
    var prev_block_range_start: usize = 0;

    const max_items_per_block = comptime filefmt.maxItemsPerBlock(filefmt.max_block_size);
    var block_hashes_buf: [max_items_per_block]u32 = undefined;
    var block_docids_buf: [max_items_per_block]u32 = undefined;

    var block_hashes: []const u32 = undefined;
    var block_docids: []const u32 = undefined;

    // Let's say we have blocks like this:
    //
    // |4.......|6.......|9.......|
    //
    // We want to find hash=2, lowerBound returns block=0 (4), so we start at that block.
    // We want to find hash=6, lowerBound returns block=1 (6), but block=0 could still contain hash=6, so we go one back.
    // We want to find hash=7, lowerBound returns block=2 (9), but block=1 could still contain hash=6, so we go one back.
    // We want to find hash=10, lowerBound returns block=3 (EOF), but block=2 could still contain hash=6, so we go one back.

    for (sorted_hashes, 1..) |hash, i| {
        var block_no = std.sort.lowerBound(u32, self.index.items[prev_block_range_start..], hash, compareHashes) + prev_block_range_start;
        if (block_no > 0) {
            block_no -= 1;
        }
        prev_block_range_start = block_no;

        var num_docs: usize = 0;
        var num_blocks: u64 = 0;
        while (block_no < self.index.items.len and self.index.items[block_no] <= hash) : (block_no += 1) {
            const is_new_block = block_no != prev_block_no;
            prev_block_no = block_no;
            const block_data = self.getBlockData(block_no);
            if (is_new_block) {
                block_hashes = try filefmt.readBlockHashesOnly(block_data, &block_hashes_buf);
            }
            const matches = std.sort.equalRange(u32, block_hashes, hash, compareHashes);
            if (matches[0] != matches[1]) {
                if (is_new_block) {
                    block_docids = try filefmt.readBlockDocidsOnly(block_data, block_hashes, &block_docids_buf, self.min_doc_id);
                }
                for (matches[0]..matches[1]) |j| {
                    try results.incr(block_docids[j], self.info.version);
                }
                num_docs += matches[1] - matches[0];
                if (num_docs > 1000) {
                    break; // XXX explain why
                }
            }
            num_blocks += 1;
        }

        metrics.scannedDocsPerHash(num_docs);
        metrics.scannedBlocksPerHash(num_blocks);

        if (i % 10 == 0) {
            try deadline.check();
        }
    }
}

pub fn load(self: *Self, info: SegmentInfo) !void {
    try filefmt.readSegmentFile(self.dir, info, self);
}

pub fn delete(self: *Self) void {
    var file_name_buf: [filefmt.max_file_name_size]u8 = undefined;
    const file_name = filefmt.buildSegmentFileName(&file_name_buf, self.info);

    log.info("deleting segment file {s}", .{file_name});

    self.dir.deleteFile(file_name) catch |err| {
        if (err != error.FileNotFound) {
            log.err("failed to clean up segment file {s}: {}", .{ file_name, err });
        }
    };
}

pub fn cleanup(self: *Self) void {
    self.delete();
}

pub fn merge(self: *Self, source: anytype) !void {
    try self.build(source);
}

pub fn build(self: *Self, source: anytype) !void {
    var file_name_buf: [filefmt.max_file_name_size]u8 = undefined;
    const file_name = filefmt.buildSegmentFileName(&file_name_buf, source.segment.info);

    try filefmt.writeSegmentFile(self.dir, source);

    errdefer self.dir.deleteFile(file_name) catch |err| {
        if (err != error.FileNotFound) {
            log.err("failed to clean up segment file {s}: {}", .{ file_name, err });
        }
    };

    try filefmt.readSegmentFile(self.dir, source.segment.info, self);
}

test "build" {
    const MemorySegment = @import("MemorySegment.zig");

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var data_dir = try tmp_dir.dir.makeOpenPath("data", .{});
    defer data_dir.close();

    var source = MemorySegment.init(std.testing.allocator, .{});
    defer source.deinit(.delete);

    source.info = .{ .version = 1 };
    source.status.frozen = true;
    try source.docs.put(source.allocator, 1, true);
    try source.items.append(source.allocator, .{ .id = 1, .hash = 1 });
    try source.items.append(source.allocator, .{ .id = 1, .hash = 2 });

    var source_reader = source.reader();
    defer source_reader.close();

    var segment = Self.init(std.testing.allocator, .{ .dir = tmp_dir.dir });
    defer segment.deinit(.delete);

    try segment.build(&source_reader);

    try std.testing.expectEqualDeep(SegmentInfo{ .version = 1, .merges = 0 }, segment.info);
    try std.testing.expectEqual(1, segment.docs.count());
    try std.testing.expectEqual(1, segment.index.items.len);
}

pub fn getSize(self: Self) usize {
    return self.num_items;
}

pub fn reader(self: *const Self) Reader {
    return .{
        .segment = self,
    };
}

pub const Reader = struct {
    segment: *const Self,
    hashes_buf: [filefmt.max_items_per_block]u32 = undefined,
    docids_buf: [filefmt.max_items_per_block]u32 = undefined,
    hashes: []const u32 = undefined,
    docids: []const u32 = undefined,
    index: usize = 0,
    block_no: usize = 0,

    pub fn close(self: *Reader) void {
        _ = self;
    }

    pub fn read(self: *Reader) !?Item {
        while (self.index >= self.hashes.len) {
            if (self.block_no >= self.segment.index.items.len) {
                return null;
            }
            self.index = 0;

            const block_data = self.segment.getBlockData(self.block_no);
            self.block_no += 1;

            self.hashes = try filefmt.readBlockHashesOnly(block_data, &self.hashes_buf);
            self.docids = try filefmt.readBlockDocidsOnly(block_data, self.hashes, &self.docids_buf, self.segment.min_doc_id);
        }

        return .{
            .hash = self.hashes[self.index],
            .id = self.docids[self.index],
        };
    }

    pub fn advance(self: *Reader) void {
        if (self.index < self.hashes.len) {
            self.index += 1;
        }
    }
};
