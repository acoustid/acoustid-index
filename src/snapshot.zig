// Node-to-node snapshot: an index's file segments + a manifest, with NO WAL and NO
// in-memory segments. It's how a new or behind replica bootstraps without replaying the
// whole changelog (see notes/bootstrap-design.md): restore it, then resume the tail
// from the coordinator at the embedded watermark — the max segment `version`, which is
// the external feed position. The segments also carry their internal commit ids, which
// order them locally but mean nothing to the coordinator.
//
// Wire form: a single msgpack `SnapshotHeader` (self-delimiting, so the reader lands
// exactly on the first payload byte), then each file segment's raw bytes concatenated
// in header order. Only the small header is msgpack; the large segment payloads are the
// resident FileSegment.data slices streamed straight to the socket, never copied through
// an intermediate buffer (std.Io.Writer forwards any slice bigger than its buffer
// directly to the drain). The manifest isn't shipped as a payload — it's reconstructed
// from the header's SegmentInfos on restore.

const std = @import("std");
const zio = @import("zio");
const msgpack = @import("msgpack");
const filefmt = @import("filefmt.zig");
const manifest = @import("manifest.zig");
const SegmentInfo = @import("segment.zig").SegmentInfo;
const Segments = @import("Index.zig").Segments;

// Bump `format` on an incompatible layout change; new optional fields are backward
// compatible on their own (msgpack map + omit_nulls).
pub const format_version: u32 = 1;

pub const SegmentEntry = struct {
    info: SegmentInfo,
    size: u64, // byte length of this segment's payload

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const SnapshotHeader = struct {
    format: u32 = format_version,
    generation: u64,
    segments: []SegmentEntry,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

// Write a snapshot of `segs` (a pinned, immutable reader snapshot — the caller keeps it
// alive) to `w`. `arena` serializes only the small header. Segment payloads are handed
// to `w` uncopied.
pub fn writeSnapshot(w: *std.Io.Writer, arena: std.mem.Allocator, segs: *const Segments, generation: u64) !void {
    const entries = try arena.alloc(SegmentEntry, segs.file.len);
    for (segs.file, 0..) |s, i| entries[i] = .{ .info = s.value.info, .size = s.value.data.len };

    var hbuf: std.Io.Writer.Allocating = .init(arena);
    defer hbuf.deinit();
    try msgpack.encode(SnapshotHeader{ .generation = generation, .segments = entries }, &hbuf.writer);
    try w.writeAll(hbuf.written());

    for (segs.file) |s| {
        try w.writeAll(s.value.data); // resident file bytes, streamed uncopied
    }
}

pub const Entry = struct { info: SegmentInfo, data: []const u8 };
pub const Parsed = struct { generation: u64, entries: []Entry };

// Parse a whole in-memory snapshot; payloads are copied into `arena`. Used by tests and
// small restores; milestone 4 adds a streaming restore straight to files for large
// snapshots. The segment file name is `filefmt.buildSegmentFileName(entry.info)`.
pub fn parse(arena: std.mem.Allocator, bytes: []const u8) !Parsed {
    var r = std.Io.Reader.fixed(bytes);
    const unpacker = msgpack.unpacker(&r, arena);
    const header = try unpacker.read(SnapshotHeader);
    if (header.format != format_version) return error.UnsupportedSnapshotFormat;

    const entries = try arena.alloc(Entry, header.segments.len);
    for (header.segments, 0..) |seg, i| {
        const data = try arena.alloc(u8, @intCast(seg.size));
        try r.readSliceAll(data);
        entries[i] = .{ .info = seg.info, .data = data };
    }
    return .{ .generation = header.generation, .entries = entries };
}

// Stream a snapshot from `r` into `dir` (an empty data dir): reconstruct + write the
// manifest from the header, then stream each segment payload straight to its file (no
// whole-archive buffering). Verifies the snapshot's generation is `expected_generation`
// — the lineage the caller means to restore. The watermark isn't returned; the caller
// opens the index, which derives both the resume position and the commit ids from the
// restored manifest.
pub fn restoreInto(dir: zio.Dir, r: *std.Io.Reader, arena: std.mem.Allocator, expected_generation: u64) !void {
    const unpacker = msgpack.unpacker(r, arena);
    const header = try unpacker.read(SnapshotHeader);
    if (header.format != format_version) return error.UnsupportedSnapshotFormat;
    if (header.generation != expected_generation) return error.SnapshotGenerationMismatch;

    const infos = try arena.alloc(SegmentInfo, header.segments.len);
    for (header.segments, 0..) |seg, i| infos[i] = seg.info;
    try manifest.write(dir, arena, infos);

    const scratch = try arena.alloc(u8, 128 * 1024);
    for (header.segments) |seg| {
        var name_buf: [filefmt.max_file_name_size]u8 = undefined;
        const name = filefmt.buildSegmentFileName(&name_buf, seg.info);
        try streamToFile(dir, name, r, seg.size, scratch);
    }
}

fn streamToFile(dir: zio.Dir, name: []const u8, r: *std.Io.Reader, size: u64, scratch: []u8) !void {
    const file = try dir.createFile(name, .{ .truncate = true });
    defer file.close();
    var remaining = size;
    var off: u64 = 0;
    while (remaining > 0) {
        const want: usize = @intCast(@min(remaining, scratch.len));
        try r.readSliceAll(scratch[0..want]);
        var written: usize = 0;
        while (written < want) written += try file.write(scratch[written..want], off + written);
        off += want;
        remaining -= want;
    }
}
