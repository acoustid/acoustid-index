// Per-index write-ahead log. Each index directory holds one append-only "oplog"
// file of msgpack-encoded Transactions, fsync'd on every append. The commit id
// is the index version (log-position-as-version); on startup the oplog is
// replayed to rebuild the in-memory segments.
//
// This is the standalone/file-backed log. In cluster mode the same transactions
// come from a PostgreSQL changelog instead; the append/replay shape is kept the
// same so the apply path is shared.
//
// For now: a single file, no rotation or truncation yet (retention comes with
// checkpointing).

const std = @import("std");
const zio = @import("zio");
const msgpack = @import("msgpack");
const log = std.log.scoped(.oplog);

const Change = @import("change.zig").Change;
const Metadata = @import("Metadata.zig");
const Transaction = @import("change.zig").Transaction;

const Self = @This();

const file_name = "oplog";

allocator: std.mem.Allocator,
file: zio.File,
write_offset: u64 = 0,
last_version: u64 = 0,

/// Open (creating if needed) the oplog file in `dir` and replay existing
/// transactions to `handler` (`fn(ctx, Transaction) !void`). The Transaction is
/// only valid for the duration of the call.
pub fn open(allocator: std.mem.Allocator, dir: zio.Dir, ctx: anytype, handler: anytype) !Self {
    const file = dir.openFile(file_name, .{ .mode = .read_write }) catch |err| switch (err) {
        error.FileNotFound => try dir.createFile(file_name, .{ .read = true }),
        else => return err,
    };
    errdefer file.close();

    var self = Self{
        .allocator = allocator,
        .file = file,
    };

    try self.replay(ctx, handler);
    return self;
}

pub fn deinit(self: *Self) void {
    self.file.close();
}

fn replay(self: *Self, ctx: anytype, handler: anytype) !void {
    var read_buf: [64 * 1024]u8 = undefined;
    var reader = self.file.reader(&read_buf);

    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();

    var count: u64 = 0;
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

    self.write_offset = reader.position;
    if (count > 0) {
        log.info("replayed {d} transactions, version {d}", .{ count, self.last_version });
    }
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

    var written: usize = 0;
    while (written < bytes.len) {
        written += try self.file.write(bytes[written..], self.write_offset + written);
    }
    try self.file.sync(.{});

    self.write_offset += bytes.len;
    self.last_version = version;
    return version;
}
