// The logical changelog interface — the operations we would implement in SQL,
// abstracted so the index/replicator can be tested without PostgreSQL.
//
// In production, fpindex does NOT connect to PG. It speaks this protocol to a
// stateless gateway (over websocket); the gateway is the only component that
// touches PG (LISTEN/NOTIFY + SELECT/INSERT). Implementations:
//   - MemoryChangelog: in-memory stub for tests (here).
//   - WebSocketChangelog: fpindex -> gateway (later; PG-free).
//   - the gateway's own PG-backed impl (later; owns pg.zig).
//
// Every implementation MUST uphold PG's invariants, so the stub tests exercise
// the real semantics:
//   - a single global monotonic `id` sequence shared across indexes (one id per
//     op, i.e. a 1000-fingerprint update consumes 1000 ids),
//   - appends for an index become visible in `id` order (PG: an advisory lock in
//     the insert transaction — so a poller never skips an uncommitted lower id),
//   - `read` returns committed entries with id > after, in id order,
//   - optimistic concurrency via `expected` (the index's current max id).

const std = @import("std");
const zio = @import("zio");
const msgpack = @import("msgpack");
const Change = @import("change.zig").Change;

/// One committed op in the shared, ordered log. `id` is the global position and
/// doubles as the index version. For an insert, `change.insert.hashes` is
/// borrowed from the Changelog and valid only until the next `read()` on the same
/// reader — copy it (e.g. into a segment) before reading on.
pub const Entry = struct {
    id: u64,
    change: Change,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

// Wire messages for the changelog-over-HTTP protocol (replica <-> coordinator).
pub const AppendRequest = struct {
    changes: []Change,
    expected: ?u64 = null,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const AppendResponse = struct {
    id: u64,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const ReadResponse = struct {
    entries: []Entry,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

/// Runtime-dispatched handle to a changelog implementation.
pub const Changelog = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Append `changes` for one index in a single commit; returns the max id
        /// assigned (the new index version). If `expected` is non-null it must
        /// equal the index's current max id or `error.VersionMismatch` is
        /// returned and nothing is appended.
        append: *const fn (ptr: *anyopaque, index_name: []const u8, changes: []const Change, expected: ?u64) anyerror!u64,

        /// Block until at least one entry with id > `after` exists for the index
        /// (or `deadline` elapses), fill `out` with entries in id order (up to
        /// `out.len`), and return how many were written. Returns 0 only on
        /// deadline. Filled entries are valid until the next `read()`.
        read: *const fn (ptr: *anyopaque, index_name: []const u8, after: u64, out: []Entry, deadline: zio.Timeout) anyerror!usize,
    };

    pub fn append(self: Changelog, index_name: []const u8, changes: []const Change, expected: ?u64) !u64 {
        return self.vtable.append(self.ptr, index_name, changes, expected);
    }

    pub fn read(self: Changelog, index_name: []const u8, after: u64, out: []Entry, deadline: zio.Timeout) !usize {
        return self.vtable.read(self.ptr, index_name, after, out, deadline);
    }
};

/// In-memory changelog stub. Upholds the PG invariants above; not durable, not
/// partitioned, no retention — those live only in the PG-backed implementation.
pub const MemoryChangelog = struct {
    allocator: std.mem.Allocator,
    mutex: zio.Mutex = .init,
    cond: zio.Condition = .init,
    rows: std.ArrayListUnmanaged(Row) = .empty,
    next_id: u64 = 1,

    const Row = struct {
        id: u64,
        index_name: []const u8, // owned
        change: Change, // insert hashes owned
    };

    pub fn init(allocator: std.mem.Allocator) MemoryChangelog {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *MemoryChangelog) void {
        for (self.rows.items) |*row| self.freeRow(row);
        self.rows.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn changelog(self: *MemoryChangelog) Changelog {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable: Changelog.VTable = .{ .append = appendImpl, .read = readImpl };

    fn appendImpl(ptr: *anyopaque, index_name: []const u8, changes: []const Change, expected: ?u64) anyerror!u64 {
        const self: *MemoryChangelog = @ptrCast(@alignCast(ptr));
        try self.mutex.lock();
        defer self.mutex.unlock();

        // Optimistic concurrency: check before mutating anything.
        if (expected) |exp| {
            if (self.maxIdForLocked(index_name) != exp) return error.VersionMismatch;
        }
        if (changes.len == 0) return self.maxIdForLocked(index_name);

        // Commit all rows atomically: on any failure, roll back to the entry state.
        const start_len = self.rows.items.len;
        const start_id = self.next_id;
        errdefer {
            for (self.rows.items[start_len..]) |*row| self.freeRow(row);
            self.rows.shrinkRetainingCapacity(start_len);
            self.next_id = start_id;
        }

        for (changes) |change| {
            const name_copy = try self.allocator.dupe(u8, index_name);
            errdefer self.allocator.free(name_copy);
            const change_copy = try dupeChange(self.allocator, change);
            errdefer freeChange(self.allocator, change_copy);
            try self.rows.append(self.allocator, .{ .id = self.next_id, .index_name = name_copy, .change = change_copy });
            self.next_id += 1;
        }

        self.cond.broadcast();
        return self.next_id - 1;
    }

    fn readImpl(ptr: *anyopaque, index_name: []const u8, after: u64, out: []Entry, deadline: zio.Timeout) anyerror!usize {
        const self: *MemoryChangelog = @ptrCast(@alignCast(ptr));
        try self.mutex.lock();
        defer self.mutex.unlock();

        while (true) {
            var n: usize = 0;
            for (self.rows.items) |row| { // stored in id order (ids are monotonic)
                if (n == out.len) break;
                if (row.id <= after) continue;
                if (!std.mem.eql(u8, row.index_name, index_name)) continue;
                out[n] = .{ .id = row.id, .change = row.change };
                n += 1;
            }
            if (n > 0) return n;

            switch (deadline) {
                .none => try self.cond.wait(&self.mutex),
                else => self.cond.timedWait(&self.mutex, deadline) catch |err| switch (err) {
                    error.Timeout => return 0,
                    else => |e| return e,
                },
            }
        }
    }

    fn maxIdForLocked(self: *MemoryChangelog, index_name: []const u8) u64 {
        var max: u64 = 0;
        for (self.rows.items) |row| {
            if (row.id > max and std.mem.eql(u8, row.index_name, index_name)) max = row.id;
        }
        return max;
    }

    fn freeRow(self: *MemoryChangelog, row: *Row) void {
        self.allocator.free(row.index_name);
        freeChange(self.allocator, row.change);
    }
};

fn dupeChange(allocator: std.mem.Allocator, change: Change) !Change {
    return switch (change) {
        .insert => |i| .{ .insert = .{ .id = i.id, .hashes = try allocator.dupe(u32, i.hashes) } },
        .delete => |d| .{ .delete = d },
    };
}

fn freeChange(allocator: std.mem.Allocator, change: Change) void {
    switch (change) {
        .insert => |i| allocator.free(i.hashes),
        .delete => {},
    }
}

// ---- tests ----

const testing = std.testing;

fn ins(id: u32, hashes: []const u32) Change {
    return .{ .insert = .{ .id = id, .hashes = hashes } };
}

test "MemoryChangelog: shared monotonic ids, one per op" {
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var cl = MemoryChangelog.init(testing.allocator);
    defer cl.deinit();
    const log = cl.changelog();

    // Two ops -> ids 1,2 -> version 2.
    try testing.expectEqual(@as(u64, 2), try log.append("a", &.{ ins(1, &.{ 10, 20 }), ins(2, &.{30}) }, null));
    // Different index shares the sequence: next id is 3.
    try testing.expectEqual(@as(u64, 3), try log.append("b", &.{ins(9, &.{40})}, null));
    // Back to "a": id 4.
    try testing.expectEqual(@as(u64, 4), try log.append("a", &.{.{ .delete = .{ .id = 1 } }}, null));
}

test "MemoryChangelog: optimistic concurrency" {
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var cl = MemoryChangelog.init(testing.allocator);
    defer cl.deinit();
    const log = cl.changelog();

    // Fresh index: expected version is 0.
    try testing.expectError(error.VersionMismatch, log.append("a", &.{ins(1, &.{10})}, 5));
    try testing.expectEqual(@as(u64, 1), try log.append("a", &.{ins(1, &.{10})}, 0));
    // Now the index is at 1; a stale expected fails and appends nothing.
    try testing.expectError(error.VersionMismatch, log.append("a", &.{ins(2, &.{20})}, 0));
    try testing.expectEqual(@as(u64, 2), try log.append("a", &.{ins(2, &.{20})}, 1));
}

test "MemoryChangelog: read fills buffer in id order, filtered by index + after" {
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var cl = MemoryChangelog.init(testing.allocator);
    defer cl.deinit();
    const log = cl.changelog();

    _ = try log.append("a", &.{ ins(1, &.{10}), ins(2, &.{20}) }, null); // ids 1,2
    _ = try log.append("b", &.{ins(3, &.{30})}, null); // id 3 (other index)
    _ = try log.append("a", &.{ins(4, &.{40})}, null); // id 4

    var buf: [8]Entry = undefined;
    // All of "a" from the start: ids 1,2,4 (not 3, which is index "b").
    const n = try log.read("a", 0, &buf, .{ .duration = .fromMilliseconds(0) });
    try testing.expectEqual(@as(usize, 3), n);
    try testing.expectEqual(@as(u64, 1), buf[0].id);
    try testing.expectEqual(@as(u64, 2), buf[1].id);
    try testing.expectEqual(@as(u64, 4), buf[2].id);
    try testing.expectEqual(@as(u32, 40), buf[2].change.insert.hashes[0]);

    // Buffer smaller than available -> capped at out.len, still id order.
    var small: [2]Entry = undefined;
    try testing.expectEqual(@as(usize, 2), try log.read("a", 0, &small, .{ .duration = .fromMilliseconds(0) }));
    try testing.expectEqual(@as(u64, 1), small[0].id);

    // After a position: only ids > 2 for "a" -> just id 4.
    try testing.expectEqual(@as(usize, 1), try log.read("a", 2, &buf, .{ .duration = .fromMilliseconds(0) }));
    try testing.expectEqual(@as(u64, 4), buf[0].id);
}

test "MemoryChangelog: read on empty times out, returns 0" {
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var cl = MemoryChangelog.init(testing.allocator);
    defer cl.deinit();
    const log = cl.changelog();

    var buf: [4]Entry = undefined;
    try testing.expectEqual(@as(usize, 0), try log.read("a", 0, &buf, .{ .duration = .fromMilliseconds(5) }));
}

test "MemoryChangelog: read blocks until a concurrent append wakes it" {
    const rt = try zio.Runtime.init(testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();

    var cl = MemoryChangelog.init(testing.allocator);
    defer cl.deinit();
    const log = cl.changelog();

    const Ctx = struct {
        fn reader(l: Changelog, got: *usize, first: *u64) !void {
            var buf: [4]Entry = undefined;
            const n = try l.read("a", 0, &buf, .none); // blocks until the append
            got.* = n;
            if (n > 0) first.* = buf[0].id;
        }
        fn appender(l: Changelog) !void {
            _ = try l.append("a", &.{ins(1, &.{ 10, 20 })}, null);
        }
    };

    var got: usize = 0;
    var first: u64 = 0;
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Ctx.reader, .{ log, &got, &first });
    try group.spawn(Ctx.appender, .{log});
    try group.wait();

    try testing.expectEqual(@as(usize, 1), got);
    try testing.expectEqual(@as(u64, 1), first);
}
