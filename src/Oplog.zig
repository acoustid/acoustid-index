// Per-index write-ahead log. Transactions are appended to rotating ".xlog" files
// named by their first commit id. The commit id is the index version
// (log-position-as-version). On startup the files are replayed in order; after a
// checkpoint, truncate() deletes files whose transactions are all durable in file
// segments.
//
// Each record is framed [u32 payload_len][u32 crc32(payload)][payload] so replay
// can detect a torn/corrupt tail (a crash mid-append) and recover the valid
// prefix instead of failing to open.
//
// This is the standalone/file-backed log. In cluster mode the same transactions
// come from a PostgreSQL changelog instead; the append/replay shape is kept the
// same so the apply path is shared. `sync` controls whether appends fsync — true
// when this log is the authoritative durable copy (standalone), false when an
// upstream (PG) owns durability.
//
// All access is serialized by the owning Index's write lock, so the oplog needs
// no internal lock.

const std = @import("std");
const zio = @import("zio");
const msgpack = @import("msgpack");
const log = std.log.scoped(.oplog);

const Change = @import("change.zig").Change;
const Metadata = @import("Metadata.zig");
const Transaction = @import("change.zig").Transaction;

const Self = @This();

const file_suffix = ".xlog";
const file_name_len = 16 + file_suffix.len;
const default_max_file_size = 16 * 1024 * 1024;
const record_header_size = 8; // u32 payload_len + u32 crc32
const max_record_size = 64 * 1024 * 1024; // sanity bound for a framed payload

pub const WriteOptions = struct {
    // Optimistic concurrency: fail with error.VersionMismatch if the current
    // version doesn't match.
    expected_version: ?u64 = null,
    // Apply at this externally-assigned version (replicated apply from an upstream
    // log); null mints the next local version.
    version: ?u64 = null,
};

allocator: std.mem.Allocator,
dir: zio.Dir,
// Whether each append fsyncs. See the file header.
sync: bool = true,
// Sorted first-commit-ids of the on-disk .xlog files.
files: std.ArrayListUnmanaged(u64) = .empty,
current_file: ?zio.File = null,
current_start: u64 = 0,
current_size: usize = 0,
write_offset: u64 = 0,
last_version: u64 = 0,
max_file_size: usize = default_max_file_size,

fn buildName(buf: []u8, start: u64) []u8 {
    return std.fmt.bufPrint(buf, "{x:0>16}" ++ file_suffix, .{start}) catch unreachable;
}

fn parseName(name: []const u8) ?u64 {
    if (name.len != file_name_len or !std.mem.endsWith(u8, name, file_suffix)) return null;
    return std.fmt.parseUnsigned(u64, name[0..16], 16) catch null;
}

/// Open the log in `dir` and replay existing transactions to `handler`
/// (`fn(ctx, Transaction) !void`). The Transaction is only valid for the call.
pub fn open(allocator: std.mem.Allocator, dir: zio.Dir, sync: bool, ctx: anytype, handler: anytype) !Self {
    var self = Self{ .allocator = allocator, .dir = dir, .sync = sync };
    errdefer self.files.deinit(allocator);

    var it = dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind != .file) continue;
        const start = parseName(entry.name) orelse continue;
        try self.files.append(allocator, start);
    }
    std.mem.sort(u64, self.files.items, {}, std.sort.asc(u64));

    try self.replay(ctx, handler);
    return self;
}

pub fn deinit(self: *Self) void {
    if (self.current_file) |file| file.close();
    self.files.deinit(self.allocator);
}

const RecordResult = union(enum) {
    record: Transaction,
    clean_eof, // exactly at a record boundary with nothing after
    torn, // incomplete or corrupt record (a crash mid-append can only tear the tail)
};

// Read one framed record. Real I/O / OOM errors propagate; a short or corrupt
// record is reported as `.torn`, a clean boundary as `.clean_eof`.
fn readRecord(reader: *std.Io.Reader, arena: std.mem.Allocator) !RecordResult {
    // Distinguish a clean boundary (nothing left) from a torn header (1-7 bytes).
    _ = reader.peekByte() catch |err| switch (err) {
        error.EndOfStream => return .clean_eof,
        else => return err,
    };

    const header = reader.takeArray(record_header_size) catch |err| switch (err) {
        error.EndOfStream => return .torn, // partial header
        else => return err,
    };
    const len = std.mem.readInt(u32, header[0..4], .little);
    const crc = std.mem.readInt(u32, header[4..8], .little);
    if (len == 0 or len > max_record_size) return .torn;

    const payload = try arena.alloc(u8, len);
    reader.readSliceAll(payload) catch |err| switch (err) {
        error.EndOfStream => return .torn, // partial payload
        else => return err,
    };
    if (std.hash.crc.Crc32.hash(payload) != crc) return .torn;

    const txn = msgpack.decodeFromSliceLeaky(Transaction, arena, payload) catch return .torn;
    return .{ .record = txn };
}

fn replay(self: *Self, ctx: anytype, handler: anytype) !void {
    var read_buf: [64 * 1024]u8 = undefined;
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();

    var count: u64 = 0;
    files: for (self.files.items) |start| {
        var name_buf: [file_name_len]u8 = undefined;
        const name = buildName(&name_buf, start);
        const file = try self.dir.openFile(name, .{ .mode = .read_only });
        defer file.close();

        var reader = file.reader(&read_buf);
        while (true) {
            _ = arena.reset(.retain_capacity);
            switch (try readRecord(&reader.interface, arena.allocator())) {
                .record => |txn| {
                    self.last_version = @max(self.last_version, txn.id);
                    try handler(ctx, txn);
                    count += 1;
                },
                .clean_eof => break, // this file ended cleanly; on to the next
                .torn => {
                    // A torn record can only be the tail (a crash mid-append writes
                    // the last record; nothing follows it). Recover the prefix and
                    // stop — a later append or the PG poller refills from here.
                    log.warn("oplog: torn record in {s}, stopping replay at version {d}", .{ name, self.last_version });
                    break :files;
                },
            }
        }
    }
    if (count > 0) {
        log.info("replayed {d} transactions, version {d}", .{ count, self.last_version });
    }
}

// Current append file, rotating to a new one (named by `version`) when full or
// on the first append after open.
fn getFile(self: *Self, version: u64) !zio.File {
    if (self.current_file) |file| {
        if (self.current_size < self.max_file_size) return file;
        file.close();
        self.current_file = null;
    }

    var name_buf: [file_name_len]u8 = undefined;
    const name = buildName(&name_buf, version);
    const file = try self.dir.createFile(name, .{ .truncate = true });
    errdefer file.close();
    try self.files.append(self.allocator, version);

    self.current_file = file;
    self.current_start = version;
    self.current_size = 0;
    self.write_offset = 0;
    return file;
}

/// Append a transaction (framed, fsync if `sync`). Returns the version: the one
/// in `options.version` (replicated apply) or the next local one. With
/// `options.expected_version` set and mismatched, fails with error.VersionMismatch
/// and writes nothing.
pub fn append(self: *Self, changes: []const Change, options: WriteOptions) !u64 {
    if (options.expected_version) |expected| {
        if (self.last_version != expected) return error.VersionMismatch;
    }
    const version = options.version orelse (self.last_version + 1);

    var w = std.Io.Writer.Allocating.init(self.allocator);
    defer w.deinit();
    try msgpack.encode(Transaction{
        .id = version,
        .changes = changes,
    }, &w.writer);
    const payload = w.written();

    var header: [record_header_size]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], @intCast(payload.len), .little);
    std.mem.writeInt(u32, header[4..8], std.hash.crc.Crc32.hash(payload), .little);

    // getFile may rotate (resetting write_offset), so call it before writing.
    const file = try self.getFile(version);
    try self.writeAll(file, &header);
    try self.writeAll(file, payload);
    if (self.sync) try file.sync(.{});

    self.last_version = version;
    return version;
}

fn writeAll(self: *Self, file: zio.File, bytes: []const u8) !void {
    var written: usize = 0;
    while (written < bytes.len) {
        written += try file.write(bytes[written..], self.write_offset + written);
    }
    self.write_offset += bytes.len;
    self.current_size += bytes.len;
}

fn cmpVersion(v: u64, item: u64) std.math.Order {
    return std.math.order(v, item);
}

/// Delete oplog files whose transactions are all below `version` (now durable in
/// file segments). Keeps the file that spans `version` and all newer files.
pub fn truncate(self: *Self, version: u64) !void {
    // First file whose start >= version; keep the one before it (it may span
    // `version`), so files strictly before that are safe to delete.
    var keep_from = std.sort.lowerBound(u64, self.files.items, version, cmpVersion);
    if (keep_from > 0) keep_from -= 1;

    var deleted: usize = 0;
    while (deleted < keep_from) : (deleted += 1) {
        const start = self.files.items[deleted];
        if (self.current_file != null and start == self.current_start) break; // never delete the open file
        var name_buf: [file_name_len]u8 = undefined;
        const name = buildName(&name_buf, start);
        self.dir.deleteFile(name) catch |err| {
            if (err != error.FileNotFound) log.warn("failed to delete oplog file {s}: {}", .{ name, err });
        };
    }
    if (deleted > 0) {
        const remaining = self.files.items.len - deleted;
        std.mem.copyForwards(u64, self.files.items[0..remaining], self.files.items[deleted..]);
        self.files.shrinkRetainingCapacity(remaining);
        log.info("truncated {d} oplog files below version {d}", .{ deleted, version });
    }
}
