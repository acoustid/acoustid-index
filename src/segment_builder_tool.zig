const std = @import("std");
const builtin = @import("builtin");

const filefmt = @import("filefmt.zig");
const SegmentInfo = @import("segment.zig").SegmentInfo;
const Item = @import("segment.zig").Item;

pub const TextSegmentReader = struct {
    allocator: std.mem.Allocator,
    items: std.ArrayList(Item),
    current_index: usize = 0,
    segment: SegmentData,

    const SegmentData = struct {
        info: SegmentInfo,
        metadata: std.StringHashMap(?[]const u8),
        docs: std.AutoHashMap(u32, bool),
        min_doc_id: u32,

        pub fn init(allocator: std.mem.Allocator, info: SegmentInfo) SegmentData {
            return .{
                .info = info,
                .metadata = std.StringHashMap(?[]const u8).init(allocator),
                .docs = std.AutoHashMap(u32, bool).init(allocator),
                .min_doc_id = std.math.maxInt(u32),
            };
        }

        pub fn deinit(self: *SegmentData) void {
            self.metadata.deinit();
            self.docs.deinit();
        }
    };

    pub fn init(allocator: std.mem.Allocator, info: SegmentInfo) !TextSegmentReader {
        var self = TextSegmentReader{
            .allocator = allocator,
            .items = std.ArrayList(Item).init(allocator),
            .current_index = 0,
            .segment = SegmentData.init(allocator, info),
        };

        // Read all data from stdin
        const stdin = std.io.getStdIn();
        var stdin_reader = std.io.bufferedReader(stdin.reader());
        var line_buffer = std.ArrayList(u8).init(allocator);
        defer line_buffer.deinit();

        while (true) {
            line_buffer.clearRetainingCapacity();
            
            stdin_reader.reader().readUntilDelimiterArrayList(&line_buffer, '\n', std.math.maxInt(usize)) catch |err| switch (err) {
                error.EndOfStream => {
                    if (line_buffer.items.len == 0) break;
                    // Process the last line without newline
                },
                else => return err,
            };

            const line = std.mem.trim(u8, line_buffer.items, " \t\r\n");
            if (line.len == 0) continue;

            var parts = std.mem.splitScalar(u8, line, ' ');
            const hash_str = parts.next() orelse continue;
            const docid_str = parts.next() orelse continue;

            const hash = std.fmt.parseInt(u32, hash_str, 10) catch continue;
            const docid = std.fmt.parseInt(u32, docid_str, 10) catch continue;

            // Track doc IDs and find minimum
            try self.segment.docs.put(docid, true);
            if (docid < self.segment.min_doc_id) {
                self.segment.min_doc_id = docid;
            }

            try self.items.append(Item{ .hash = hash, .id = docid });
        }

        return self;
    }

    pub fn deinit(self: *TextSegmentReader) void {
        self.items.deinit();
        self.segment.deinit();
    }

    pub fn read(self: *TextSegmentReader) !?Item {
        if (self.current_index >= self.items.items.len) {
            return null;
        }
        const item = self.items.items[self.current_index];
        return item;
    }

    pub fn advance(self: *TextSegmentReader) void {
        self.current_index += 1;
    }

    pub fn close(_: *TextSegmentReader) void {
        // No-op for this reader
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 4) {
        std.debug.print("Usage: {s} <output_dir> <version> <merges>\n", .{args[0]});
        std.debug.print("Reads hash/docid pairs from stdin and creates a modern segment file\n", .{});
        std.debug.print("Example: cat data.txt | {s} /tmp/segments 1 0\n", .{args[0]});
        std.process.exit(1);
    }

    const output_dir_path = args[1];
    const version = try std.fmt.parseInt(u64, args[2], 10);
    const merges = try std.fmt.parseInt(u32, args[3], 10);

    var output_dir = try std.fs.cwd().openDir(output_dir_path, .{});
    defer output_dir.close();

    const info = SegmentInfo{ .version = version, .merges = merges };

    var reader = try TextSegmentReader.init(allocator, info);
    defer reader.deinit();

    try filefmt.writeSegmentFile(output_dir, &reader);

    std.debug.print("Successfully created segment file for version={}, merges={}\n", .{ version, merges });
}