// Segment file format (v1 of the rewrite; no backwards compat with the old
// project). Layout, in order:
//   1. Header  - msgpack: magic, segment info, block_size, has_metadata/docs
//   2. Metadata - msgpack string->string map
//   3. Docs     - msgpack doc_id -> alive? map
//   4. Padding  - zeros to the next block_size boundary
//   5. Blocks   - fixed-size StreamVByte-compressed (hash, id) blocks, terminated
//                 by one empty block (num_items == 0) for SIMD read padding
//   6. Block index - little-endian u32 max_hash per block
//   7. Footer   - msgpack: magic, num_items, num_blocks, checksum
//   8. Footer size - little-endian u32
//
// Written whole from an in-memory buffer via zio.File (atomic temp+rename), and
// read whole into an aligned heap buffer (mlock'd anonymous memory comes later).

const std = @import("std");
const zio = @import("zio");
const assert = std.debug.assert;
const log = std.log.scoped(.filefmt);

const msgpack = @import("msgpack");
const Item = @import("segment.zig").Item;
const SegmentInfo = @import("segment.zig").SegmentInfo;
const FileSegment = @import("FileSegment.zig");
const block = @import("block.zig");
const BlockEncoder = block.BlockEncoder;
const decodeBlockHeader = block.decodeBlockHeader;

pub const default_block_size = 512;
pub const min_block_size = block.MIN_BLOCK_SIZE;
pub const max_block_size = block.MAX_BLOCK_SIZE;

pub const max_file_name_size = 64;
const segment_file_suffix = ".data";
const segment_file_name_fmt = "{x:0>16}-{x:0>8}" ++ segment_file_suffix;

const header_magic: u32 = 0x53474D31; // "SGM1"
const footer_magic: u32 = @byteSwap(header_magic);

// Alignment of the loaded segment buffer: >= u32 (block index cast) and cache
// friendly; a stepping stone toward page-aligned mlock'd memory later.
const segment_align: std.mem.Alignment = .fromByteUnits(64);

pub fn buildSegmentFileName(buf: []u8, info: SegmentInfo) []u8 {
    assert(buf.len == max_file_name_size);
    return std.fmt.bufPrint(buf, segment_file_name_fmt, .{ info.version, info.merges }) catch unreachable;
}

pub fn isSegmentFileName(name: []const u8) bool {
    return parseSegmentFileName(name) != null;
}

pub fn parseSegmentFileName(name: []const u8) ?SegmentInfo {
    if (!std.mem.endsWith(u8, name, segment_file_suffix)) return null;
    const s = name[0 .. name.len - segment_file_suffix.len];
    if (s.len != 25 or s[16] != '-') return null;
    const version = std.fmt.parseUnsigned(u64, s[0..16], 16) catch return null;
    const merges = std.fmt.parseUnsigned(u32, s[17..25], 16) catch return null;
    return SegmentInfo{ .version = version, .merges = merges };
}

pub const SegmentFileHeader = struct {
    magic: u32,
    info: SegmentInfo,
    has_metadata: bool,
    has_docs: bool,
    block_size: u32,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .field_index } };
    }
};

pub const SegmentFileFooter = struct {
    magic: u32,
    num_items: u32,
    num_blocks: u32,
    checksum: u64,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .field_index } };
    }
};

const WriteBlocksResult = struct {
    footer: SegmentFileFooter,
    max_hashes: []u32,
};

fn writeBlocks(seg_reader: anytype, writer: *std.Io.Writer, min_doc_id: u32, comptime block_size: u32, allocator: std.mem.Allocator) !WriteBlocksResult {
    var encoder = BlockEncoder.init();
    var items_buffer: [block.MAX_ITEMS_PER_BLOCK]Item = undefined;
    var items_in_buffer: usize = 0;
    var num_items: u32 = 0;
    var num_blocks: u32 = 0;
    var crc = std.hash.crc.Crc64Xz.init();
    var block_data: [block_size]u8 = undefined;
    var max_hashes: std.ArrayListUnmanaged(u32) = .empty;
    errdefer max_hashes.deinit(allocator);

    while (true) {
        while (items_in_buffer < items_buffer.len) {
            const item = try seg_reader.read() orelse break;
            items_buffer[items_in_buffer] = item;
            items_in_buffer += 1;
            seg_reader.advance();
        }

        const items_consumed = try encoder.encodeBlock(items_buffer[0..items_in_buffer], min_doc_id, &block_data);
        try writer.writeAll(&block_data);
        if (items_consumed == 0) break; // empty terminator block written above

        try max_hashes.append(allocator, items_buffer[items_consumed - 1].hash);
        num_items += @intCast(items_consumed);
        num_blocks += 1;
        crc.update(&block_data);

        const remaining = items_in_buffer - items_consumed;
        if (remaining > 0) {
            std.mem.copyForwards(Item, items_buffer[0..remaining], items_buffer[items_consumed..items_in_buffer]);
        }
        items_in_buffer = remaining;
    }

    return .{
        .footer = .{
            .magic = footer_magic,
            .num_items = num_items,
            .num_blocks = num_blocks,
            .checksum = crc.final(),
        },
        .max_hashes = try max_hashes.toOwnedSlice(allocator),
    };
}

/// Write `seg_reader`'s segment to `dir` as an immutable segment file (atomic
/// temp + rename). `seg_reader` yields sorted Items via read()/advance() and
/// exposes `.segment` (info, metadata, docs, min_doc_id).
pub fn writeSegment(dir: zio.Dir, seg_reader: anytype, allocator: std.mem.Allocator) !void {
    const segment = seg_reader.segment;
    const block_size = default_block_size;

    var name_buf: [max_file_name_size]u8 = undefined;
    const name = buildSegmentFileName(&name_buf, segment.info);

    var w = std.Io.Writer.Allocating.init(allocator);
    defer w.deinit();
    const writer = &w.writer;
    const packer = msgpack.packer(writer);

    try packer.write(SegmentFileHeader{
        .magic = header_magic,
        .info = segment.info,
        .has_metadata = true,
        .has_docs = true,
        .block_size = block_size,
    });
    try packer.writeMap(segment.metadata.entries);
    try packer.writeMap(segment.docs);

    const rem = w.written().len % block_size;
    if (rem != 0) try writer.splatByteAll(0, block_size - rem);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const result = try writeBlocks(seg_reader, writer, segment.min_doc_id, block_size, arena.allocator());

    for (result.max_hashes) |max_hash| {
        try writer.writeInt(u32, max_hash, .little);
    }

    const footer_start = w.written().len;
    try packer.write(result.footer);
    const footer_size: u32 = @intCast(w.written().len - footer_start);
    try writer.writeInt(u32, footer_size, .little);

    const bytes = w.written();

    var tmp_buf: [max_file_name_size + 4]u8 = undefined;
    const tmp_name = std.fmt.bufPrint(&tmp_buf, "{s}.tmp", .{name}) catch unreachable;

    const file = try dir.createFile(tmp_name, .{ .truncate = true });
    {
        errdefer {
            file.close();
            zio.beginShield();
            defer zio.endShield();
            dir.deleteFile(tmp_name) catch |err| {
                log.warn("failed to remove temp segment file: {}", .{err});
            };
        }
        var written: usize = 0;
        while (written < bytes.len) {
            written += try file.write(bytes[written..], written);
        }
        try file.sync(.{});
    }
    file.close();
    try dir.rename(tmp_name, dir, name);

    log.info("wrote segment {s} ({} blocks, {} items)", .{ name, result.footer.num_blocks, result.footer.num_items });
}

/// Read the segment file for `info` from `dir` into `segment` (heap-resident).
pub fn readSegment(dir: zio.Dir, info: SegmentInfo, segment: *FileSegment) !void {
    var name_buf: [max_file_name_size]u8 = undefined;
    const name = buildSegmentFileName(&name_buf, info);

    segment.dir = dir;

    const st = try dir.statPath(name);
    const file_size: usize = @intCast(st.size);

    const file = try dir.openFile(name, .{ .mode = .read_only });
    defer file.close();

    const data = try segment.allocator.alignedAlloc(u8, segment_align, file_size);
    errdefer segment.allocator.free(data);
    var off: usize = 0;
    while (off < file_size) {
        const n = try file.read(data[off..], off);
        if (n == 0) return error.UnexpectedEndOfFile;
        off += n;
    }
    segment.data = data;

    var reader = std.Io.Reader.fixed(data);
    const unpacker = msgpack.unpacker(&reader, segment.allocator);

    const header = try unpacker.read(SegmentFileHeader);
    if (header.magic != header_magic) return error.InvalidSegment;
    if (header.block_size < min_block_size or header.block_size > max_block_size) return error.InvalidSegment;

    segment.info = header.info;
    segment.block_size = header.block_size;

    if (header.has_metadata) try unpacker.readMapInto(&segment.metadata.entries);
    if (header.has_docs) try unpacker.readMapInto(&segment.docs);

    segment.min_doc_id = 0;
    segment.max_doc_id = 0;
    var it = segment.docs.keyIterator();
    while (it.next()) |k| {
        if (segment.min_doc_id == 0 or k.* < segment.min_doc_id) segment.min_doc_id = k.*;
        if (segment.max_doc_id == 0 or k.* > segment.max_doc_id) segment.max_doc_id = k.*;
    }

    const block_size = header.block_size;
    const padded = std.mem.alignForward(usize, reader.seek, block_size);
    const blocks_start = padded;

    var num_items: u32 = 0;
    var num_blocks: u32 = 0;
    var crc = std.hash.crc.Crc64Xz.init();
    var ptr = blocks_start;
    while (ptr + block_size <= data.len) {
        const block_data = data[ptr .. ptr + block_size];
        ptr += block_size;
        const bh = decodeBlockHeader(block_data);
        if (bh.num_items == 0) break; // empty terminator block
        num_items += bh.num_items;
        num_blocks += 1;
        crc.update(block_data);
    }
    const blocks_end = ptr;
    segment.blocks = data[blocks_start..blocks_end];
    segment.num_blocks = num_blocks;
    segment.num_items = num_items;

    const block_index_start = blocks_end;
    const block_index_end = block_index_start + num_blocks * @sizeOf(u32);
    if (block_index_end > data.len) return error.InvalidSegment;
    segment.block_index = @as([*]const u32, @ptrCast(@alignCast(data[block_index_start..].ptr)))[0..num_blocks];

    var footer_reader = std.Io.Reader.fixed(data[block_index_end..]);
    const footer_unpacker = msgpack.unpacker(&footer_reader, segment.allocator);
    const footer = try footer_unpacker.read(SegmentFileFooter);
    if (footer.magic != footer_magic) return error.InvalidSegment;
    if (footer.num_items != num_items or footer.num_blocks != num_blocks) return error.InvalidSegment;
    if (footer.checksum != crc.final()) return error.ChecksumMismatch;
}

pub fn deleteSegmentFile(dir: zio.Dir, info: SegmentInfo) !void {
    var name_buf: [max_file_name_size]u8 = undefined;
    const name = buildSegmentFileName(&name_buf, info);
    try dir.deleteFile(name);
}

test "segment round-trip: write, read, search" {
    const MemorySegment = @import("MemorySegment.zig");
    const Change = @import("change.zig").Change;
    const SearchResults = @import("common.zig").SearchResults;

    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const cwd = zio.Dir.cwd();
    const dir_path = "test_segment_roundtrip";
    cwd.createDir(dir_path, 0o755) catch {};
    var dir = try cwd.openDir(dir_path, .{});
    const info: SegmentInfo = .{ .version = 1, .merges = 0 };
    defer {
        deleteSegmentFile(dir, info) catch {};
        dir.close();
        cwd.deleteDir(dir_path) catch {};
    }

    var mem = MemorySegment.init(std.testing.allocator, .{});
    defer mem.deinit(.delete);
    mem.info = info;
    try mem.build(&[_]Change{
        .{ .insert = .{ .id = 1, .hashes = &[_]u32{ 100, 200, 300 } } },
        .{ .insert = .{ .id = 2, .hashes = &[_]u32{ 100, 200 } } },
    });

    var mem_reader = mem.reader();
    defer mem_reader.close();
    try writeSegment(dir, &mem_reader, std.testing.allocator);

    var seg = FileSegment.init(std.testing.allocator);
    defer seg.deinit(.delete);
    try readSegment(dir, info, &seg);

    try std.testing.expectEqual(@as(usize, 2), seg.docs.count());
    try std.testing.expectEqual(@as(u64, 1), seg.info.version);
    try std.testing.expectEqual(@as(usize, 5), seg.num_items);

    var results = SearchResults.init(std.testing.allocator, .{ .max_results = 10, .min_score = 1 });
    defer results.deinit();
    try seg.search(&[_]u32{ 100, 200, 300 }, &results);

    try std.testing.expectEqual(@as(u32, 3), results.hits.get(1).?.score);
    try std.testing.expectEqual(@as(u32, 2), results.hits.get(2).?.score);
}
