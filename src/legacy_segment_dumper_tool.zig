const std = @import("std");

pub const BLOCK_SIZE = 512;

pub const SegmentIndex = struct {
    keys: []u32,
    block_count: usize,

    pub fn init(allocator: std.mem.Allocator, block_count: usize) !*SegmentIndex {
        const self = try allocator.create(SegmentIndex);
        self.* = .{
            .keys = try allocator.alloc(u32, block_count),
            .block_count = block_count,
        };
        return self;
    }

    pub fn deinit(self: *SegmentIndex, allocator: std.mem.Allocator) void {
        allocator.free(self.keys);
        allocator.destroy(self);
    }
};

pub const SegmentDataBlock = struct {
    item_count: u16,
    data: []u8,

    pub fn deinit(self: *SegmentDataBlock, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
    }
};

pub const SegmentDataParser = struct {
    allocator: std.mem.Allocator,
    block_size: usize,

    pub fn init(allocator: std.mem.Allocator, block_size: usize) SegmentDataParser {
        return .{
            .allocator = allocator,
            .block_size = block_size,
        };
    }

    pub fn parseBlock(self: *SegmentDataParser, data: []const u8) !SegmentDataBlock {
        if (data.len < 2) return error.InvalidBlockData;

        var stream = std.io.fixedBufferStream(data);
        var reader = stream.reader();

        const item_count = try reader.readInt(u16, .big);
        const block_data = try self.allocator.alloc(u8, self.block_size - 2);
        @memcpy(block_data, data[2..]);

        return SegmentDataBlock{
            .item_count = item_count,
            .data = block_data,
        };
    }

    const Item = struct { key: u32, value: u32 };

    pub fn parseBlockItemsWithFirstKey(self: *SegmentDataParser, block: SegmentDataBlock, first_key: u32) ![]const Item {
        var items = std.ArrayList(Item).init(self.allocator);
        defer items.deinit();

        var stream = std.io.fixedBufferStream(block.data);
        var reader = stream.reader();

        var last_key: u32 = first_key;
        var last_value: u32 = 0;

        for (0..block.item_count) |_| {
            if (items.items.len == 0) {
                const value_delta = try readVInt(&reader);
                last_value = value_delta;
                try items.append(.{ .key = last_key, .value = last_value });
            } else {
                const key_delta = try readVInt(&reader);
                const value_delta = try readVInt(&reader);
                last_key += key_delta;
                if (key_delta > 0) {
                    // Key changed, value_delta is absolute
                    last_value = value_delta;
                } else {
                    // Same key, value_delta is relative
                    last_value += value_delta;
                }
                try items.append(.{ .key = last_key, .value = last_value });
            }
        }

        const result = try self.allocator.alloc(Item, items.items.len);
        @memcpy(result, items.items);
        return result;
    }
};

fn readVInt(reader: anytype) !u32 {
    var result: u32 = 0;
    var shift: u5 = 0;

    while (shift < 32) {
        const byte = try reader.readByte();
        result |= @as(u32, byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) {
            return result;
        }
        shift += 7;
    }

    return error.InvalidVInt;
}

pub const SegmentIndexParser = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) SegmentIndexParser {
        return .{ .allocator = allocator };
    }

    pub fn parse(self: *SegmentIndexParser, data: []const u8) !*SegmentIndex {
        const block_count = data.len / @sizeOf(u32);
        var segment = try SegmentIndex.init(self.allocator, block_count);

        var stream = std.io.fixedBufferStream(data);
        var reader = stream.reader();

        for (0..block_count) |i| {
            segment.keys[i] = try reader.readInt(u32, .big);
        }

        return segment;
    }
};

pub const SegmentDumper = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) SegmentDumper {
        return .{ .allocator = allocator };
    }

    pub fn dumpSegmentFiles(self: *SegmentDumper, data_file_path: []const u8, index_file_path: []const u8) !void {
        const data_file = try std.fs.cwd().openFile(data_file_path, .{});
        defer data_file.close();

        const index_file = try std.fs.cwd().openFile(index_file_path, .{});
        defer index_file.close();

        const data_stat = try data_file.stat();

        // Parse index file
        const index_data = try index_file.readToEndAlloc(self.allocator, std.math.maxInt(usize));
        defer self.allocator.free(index_data);

        var index_parser = SegmentIndexParser.init(self.allocator);
        const segment_index = try index_parser.parse(index_data);
        defer segment_index.deinit(self.allocator);

        // Parse all data blocks
        const data_data = try data_file.readToEndAlloc(self.allocator, std.math.maxInt(usize));
        defer self.allocator.free(data_data);

        var data_parser = SegmentDataParser.init(self.allocator, BLOCK_SIZE);
        const num_blocks = data_stat.size / BLOCK_SIZE;

        for (0..num_blocks) |i| {
            const block_start = i * BLOCK_SIZE;
            const block_end = block_start + BLOCK_SIZE;
            if (block_end > data_data.len) break;

            const block_data = data_data[block_start..block_end];
            var block = try data_parser.parseBlock(block_data);
            defer block.deinit(self.allocator);

            if (block.item_count > 0) {
                const first_key = segment_index.keys[i];
                const items = try data_parser.parseBlockItemsWithFirstKey(block, first_key);
                defer self.allocator.free(items);

                const stdout = std.io.getStdOut().writer();
                for (items) |item| {
                    try stdout.print("{} {}\n", .{ item.key, item.value });
                }
            }
        }
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 3) {
        std.debug.print("Usage: {s} <data_file.fid> <index_file.fii>\n", .{args[0]});
        std.debug.print("Example: {s} segment_4386687.fid segment_4386687.fii\n", .{args[0]});
        std.process.exit(1);
    }

    var dumper = SegmentDumper.init(allocator);
    try dumper.dumpSegmentFiles(args[1], args[2]);
}
