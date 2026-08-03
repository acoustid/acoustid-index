// Per-index write-ahead log. Transactions are appended to rotating ".xlog" files
// named by their first commit id. Commit ids are minted here, one per transaction,
// and are dense — segments tile them, which SegmentInfo.merge asserts on. Each
// transaction also carries the upstream changelog `version` it corresponds to; the
// two are NOT the same number (see SegmentInfo). On startup the files are replayed in
// order; after a checkpoint, truncate() deletes files whose transactions are all
// durable in file segments.
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

fn validateRecordSize(len: usize) !void {
    if (len > max_record_size) return error.RecordTooLarge;
}

pub const WriteOptions = struct {
    // Optimistic concurrency: fail with error.VersionMismatch if the index is not at
    // this version. Only reachable in standalone mode (a replicated apply never sets
    // it), where a commit's version is its own commit id.
    expected_version: ?u64 = null,
    // The upstream changelog position this commit corresponds to (replicated apply).
    // null means there is no upstream — standalone writes take the commit id itself,
    // so the two coincide exactly as they did before they were separated.
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
last_commit_id: u64 = 0,
last_version: u64 = 0,
max_file_size: usize = default_max_file_size,
write_failed: bool = false,

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
    if (len == 0) return .torn;
    try validateRecordSize(len);

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
    for (self.files.items, 0..) |start, file_index| {
        if (count > 0 and start != self.last_commit_id + 1) return error.CorruptOplog;

        var name_buf: [file_name_len]u8 = undefined;
        const name = buildName(&name_buf, start);
        const file = try self.dir.openFile(name, .{ .mode = .read_only });
        defer file.close();

        var reader = file.reader(&read_buf);
        while (true) {
            _ = arena.reset(.retain_capacity);
            switch (try readRecord(&reader.interface, arena.allocator())) {
                .record => |txn| {
                    const expected_commit_id = if (count == 0) start else self.last_commit_id + 1;
                    if (txn.id != expected_commit_id) return error.CorruptOplog;
                    self.last_commit_id = txn.id;
                    // A locally-minted commit has no stored position: its version is
                    // its commit id, which is the standalone identity.
                    self.last_version = @max(self.last_version, txn.version orelse txn.id);
                    try handler(ctx, txn);
                    count += 1;
                },
                .clean_eof => break, // this file ended cleanly; on to the next
                .torn => {
                    // A later file is possible if an older version recovered a torn
                    // tail, appended to a new file, but left the bad bytes in place.
                    // It is only continuous if it starts with the next dense commit.
                    if (file_index + 1 < self.files.items.len and
                        self.files.items[file_index + 1] != self.last_commit_id + 1)
                    {
                        return error.CorruptOplog;
                    }
                    log.warn("oplog: ignoring torn tail in {s} after commit {d}", .{ name, self.last_commit_id });
                    break;
                },
            }
        }
    }
    if (count > 0) {
        log.info("replayed {d} transactions, commit {d}", .{ count, self.last_commit_id });
    }
}

// Current append file, rotating to a new one (named by `commit_id`) when full or
// on the first append after open.
fn getFile(self: *Self, commit_id: u64) !zio.File {
    if (self.current_file) |file| {
        if (self.current_size < self.max_file_size) return file;
        file.close();
        self.current_file = null;
    }

    var name_buf: [file_name_len]u8 = undefined;
    const name = buildName(&name_buf, commit_id);
    const file = try self.dir.createFile(name, .{ .truncate = true });
    errdefer file.close();
    try self.files.append(self.allocator, commit_id);

    self.current_file = file;
    self.current_start = commit_id;
    self.current_size = 0;
    self.write_offset = 0;
    return file;
}

/// The identity of a committed transaction: its dense internal commit id and the
/// upstream changelog position it covers.
pub const Commit = struct {
    commit_id: u64,
    version: u64,
};

/// Append a transaction (framed, fsync if `sync`). Returns the minted commit id and
/// the version it carries. With `options.expected_version` set and mismatched,
/// fails with error.VersionMismatch and writes nothing.
pub fn append(self: *Self, changes: []const Change, options: WriteOptions) !Commit {
    if (self.write_failed) return error.OplogWriteFailed;
    if (options.expected_version) |expected| {
        if (self.last_version != expected) return error.VersionMismatch;
    }
    // Commit ids are always minted locally and densely. They used to be overridden
    // with the upstream position, which broke the density SegmentInfo.merge asserts
    // on as soon as a consumer coalesced a batch (one commit spanning many positions).
    const commit_id = self.last_commit_id + 1;
    // Without an upstream position, continue the version sequence rather than taking
    // the commit id: on an index that has consumed a feed the two are far apart, and
    // `orelse commit_id` would drag the version back to it.
    const version = options.version orelse (self.last_version + 1);

    // The version is a resume point and a watermark other nodes act on, so it must
    // never go backwards: a regression makes a consumer re-read from an earlier
    // position and makes this node advertise a watermark it has already passed.
    // Non-decreasing rather than strictly increasing — several commits legitimately
    // share one position when a bootstrap loads a snapshot taken at a single point.
    // Checked before anything is written, so a rejected append leaves no trace.
    if (version < self.last_version) return error.VersionWentBackwards;

    var w = std.Io.Writer.Allocating.init(self.allocator);
    defer w.deinit();
    try msgpack.encode(Transaction{
        .id = commit_id,
        // Stored as given: null for a local commit, so replay can tell the two apart
        // without a second field that could contradict this one.
        .version = options.version,
        .changes = changes,
    }, &w.writer);
    const payload = w.written();
    try validateRecordSize(payload.len);

    var header: [record_header_size]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], @intCast(payload.len), .little);
    std.mem.writeInt(u32, header[4..8], std.hash.crc.Crc32.hash(payload), .little);

    // getFile may rotate (resetting write_offset), so call it before writing.
    const file = try self.getFile(commit_id);
    self.writeAll(file, &header) catch |err| {
        self.markWriteFailed();
        return err;
    };
    self.writeAll(file, payload) catch |err| {
        self.markWriteFailed();
        return err;
    };
    if (self.sync) file.sync(.{}) catch |err| {
        self.markWriteFailed();
        return err;
    };

    self.last_commit_id = commit_id;
    self.last_version = version;
    return .{ .commit_id = commit_id, .version = version };
}

fn writeAll(self: *Self, file: zio.File, bytes: []const u8) !void {
    var written: usize = 0;
    while (written < bytes.len) {
        const n = try file.write(bytes[written..], self.write_offset);
        if (n == 0) return error.UnexpectedWriteZero;
        written += n;
        self.write_offset += n;
        self.current_size += n;
    }
}

fn markWriteFailed(self: *Self) void {
    self.write_failed = true;
    if (self.current_file) |file| file.close();
    self.current_file = null;
    log.err("oplog: append failed; refusing further writes until restart", .{});
}

fn cmpVersion(v: u64, item: u64) std.math.Order {
    return std.math.order(v, item);
}

test "oplog enforces the replay record limit before append" {
    try validateRecordSize(max_record_size);
    try std.testing.expectError(error.RecordTooLarge, validateRecordSize(max_record_size + 1));
}

/// Delete oplog files whose transactions are all below `commit_id` (now durable in
/// file segments). Keeps the file that spans `commit_id` and all newer files.
pub fn truncate(self: *Self, commit_id: u64) !void {
    // First file whose start >= commit_id; keep the one before it (it may span
    // `commit_id`), so files strictly before that are safe to delete.
    var keep_from = std.sort.lowerBound(u64, self.files.items, commit_id, cmpVersion);
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
        log.info("truncated {d} oplog files below commit {d}", .{ deleted, commit_id });
    }
}
