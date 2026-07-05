// Per-index write-ahead log. Transactions are appended (msgpack, fsync per
// append) to rotating ".xlog" files named by their first commit id. The commit
// id is the index version (log-position-as-version). On startup the files are
// replayed in order; after a checkpoint, truncate() deletes files whose
// transactions are all durable in file segments.
//
// This is the standalone/file-backed log. In cluster mode the same transactions
// come from a PostgreSQL changelog instead; the append/replay shape is kept the
// same so the apply path is shared.
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

allocator: std.mem.Allocator,
dir: zio.Dir,
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
pub fn open(allocator: std.mem.Allocator, dir: zio.Dir, ctx: anytype, handler: anytype) !Self {
    var self = Self{ .allocator = allocator, .dir = dir };
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

fn replay(self: *Self, ctx: anytype, handler: anytype) !void {
    var read_buf: [64 * 1024]u8 = undefined;
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();

    var count: u64 = 0;
    for (self.files.items) |start| {
        var name_buf: [file_name_len]u8 = undefined;
        const name = buildName(&name_buf, start);
        const file = try self.dir.openFile(name, .{ .mode = .read_only });
        defer file.close();

        var reader = file.reader(&read_buf);
        while (true) {
            _ = arena.reset(.retain_capacity);
            const txn = msgpack.decodeLeaky(Transaction, arena.allocator(), &reader.interface) catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };
            self.last_version = @max(self.last_version, txn.id);
            try handler(ctx, txn);
            count += 1;
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

/// Append a transaction and fsync. Returns the assigned version. If
/// `expected_version` is set and doesn't match the current version, fails with
/// error.VersionMismatch and writes nothing.
pub fn append(self: *Self, changes: []const Change, metadata: ?Metadata, expected_version: ?u64) !u64 {
    if (expected_version) |expected| {
        if (self.last_version != expected) return error.VersionMismatch;
    }
    const version = self.last_version + 1;

    var w = std.Io.Writer.Allocating.init(self.allocator);
    defer w.deinit();
    try msgpack.encode(Transaction{
        .id = version,
        .changes = changes,
        .metadata = metadata,
    }, &w.writer);
    const bytes = w.written();

    const file = try self.getFile(version);
    var written: usize = 0;
    while (written < bytes.len) {
        written += try file.write(bytes[written..], self.write_offset + written);
    }
    try file.sync(.{});

    self.write_offset += bytes.len;
    self.current_size += bytes.len;
    self.last_version = version;
    return version;
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
