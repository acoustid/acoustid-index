// The logical changelog interface — the operations we would implement in SQL,
// abstracted so the index/replicator can be tested without PostgreSQL.
//
// In production, fpindex does NOT connect to PG. It speaks this protocol to a
// stateless gateway (over websocket); the gateway is the only component that
// touches PG (LISTEN/NOTIFY + SELECT/INSERT). Implementations:
//   - MemoryCoordinator: in-memory stub for tests (here).
//   - WebSocketChangelog: fpindex -> gateway (later; PG-free).
//   - the gateway's own PG-backed impl (later; owns pg.zig).
//
// The model is keyed by (index_name, generation) — a lineage. A create mints a new
// generation (the create op's meta pos); delete/recreate always bumps it, so each
// lineage is physically separate. The data feed for a lineage has its OWN sequence,
// which is the index version. (The PG impl may normalize (name, generation) to a
// surrogate `index_id` on each row — that's a storage detail below this interface;
// nothing here or above ever sees it.)
//
// Invariants every implementation MUST uphold (the stub tests exercise them):
//   - the meta feed is a single global, ordered `pos` sequence (index discovery);
//     for a create, `pos` IS the generation,
//   - each lineage's data feed has its own `seq` starting at 1 (one seq per op; a
//     1000-fingerprint update consumes 1000 seqs) — this seq is the index version,
//   - `read(name, generation, after)` returns that lineage's committed entries with
//     seq > after, in seq order (lineage isolation is the generation scope — no
//     start_position, no cross-lineage bleed),
//   - `append` commits a batch atomically and honors optimistic `expected` (the
//     lineage's current max seq).

const std = @import("std");
const zio = @import("zio");
const msgpack = @import("msgpack");
const Change = @import("change.zig").Change;
const MetadataEntry = @import("change.zig").MetadataEntry;

/// One committed op in a lineage's ordered data feed. `id` is the per-lineage seq
/// and doubles as the index version. For an insert, `change.insert.hashes` is
/// borrowed from the Coordinator and valid only until the next `read()` on the same
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

/// An index-lifecycle op on the (never-truncated) global meta feed. `pos` orders
/// the feed and is the create-your-writes watermark; for a create it also IS the
/// generation (the lineage identity — orthogonal to data seqs, always increasing
/// across delete/recreate, and the durable reconcile key).
pub const MetaOp = struct {
    pos: u64,
    kind: Kind,
    index_name: []const u8,

    pub const Kind = enum(u8) { create, delete };

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const MetaReadResponse = struct {
    ops: []MetaOp,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const MetaCreateResponse = struct {
    generation: u64,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const MetaDeleteResponse = struct {
    pos: u64,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

// ---- Replica registry (peer discovery for snapshot bootstrap) ----
// Replicas periodically report their state; the coordinator is the rendezvous point
// that a bootstrapping node queries for a donor. See notes/bootstrap-design.md.

/// A replica's state for one lineage it holds locally.
pub const LineageStatus = struct {
    index_name: []const u8,
    generation: u64,
    applied: u64, // highest data-feed seq applied (the index version)
    file_version: u64, // checkpointed watermark — a snapshot from this replica resumes here

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

/// A replica's periodic heartbeat: who it is, where to fetch its snapshots, and what
/// it holds. Replaces the replica's previous status; absence past a timeout = dead.
pub const ReplicaStatus = struct {
    replica_id: []const u8,
    advertise_addr: []const u8, // base URL other nodes fetch GET /:index/_snapshot from
    lineages: []const LineageStatus,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

/// Where a bootstrapping node should fetch a snapshot, and the watermark it will land
/// on (so it resumes the data feed from there).
pub const DonorInfo = struct {
    advertise_addr: []const u8,
    file_version: u64,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

pub const DonorResponse = struct {
    donor: ?DonorInfo = null,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .{ .field_name_prefix = 1 } } };
    }
};

/// Runtime-dispatched handle to a changelog implementation.
pub const Coordinator = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Append `changes` to the (`index_name`, `generation`) lineage's data feed
        /// in a single commit; returns the max seq assigned (the new index
        /// version). If `expected` is non-null it must equal the lineage's current
        /// max seq or `error.VersionMismatch` is returned and nothing is appended.
        append: *const fn (ptr: *anyopaque, index_name: []const u8, generation: u64, changes: []const Change, expected: ?u64) anyerror!u64,

        /// Block until at least one entry with seq > `after` exists for the
        /// (`index_name`, `generation`) lineage (or `deadline` elapses), fill `out`
        /// with entries in seq order (up to `out.len`), and return how many were
        /// written. Returns 0 only on deadline. Filled entries are valid until the
        /// next `read()`.
        read: *const fn (ptr: *anyopaque, index_name: []const u8, generation: u64, after: u64, out: []Entry, deadline: zio.Timeout) anyerror!usize,

        // Meta feed (index registry / lifecycle) — global, never truncated.
        /// Record an index create; returns its generation (the create op's meta
        /// pos). Idempotent: if the name is currently active, returns the existing
        /// generation without appending a duplicate.
        createIndex: *const fn (ptr: *anyopaque, name: []const u8) anyerror!u64,
        /// Record an index delete; returns the delete op's meta `pos` (the
        /// create-your-writes watermark). No-op if the name isn't active — then it
        /// returns the current latest meta pos (already applied), so a waiter on it
        /// returns immediately.
        deleteIndex: *const fn (ptr: *anyopaque, name: []const u8) anyerror!u64,
        /// Block until a meta op with pos > `after` exists (or `deadline`), fill
        /// `out` in pos order, return the count. Ops are valid until the next call.
        readMeta: *const fn (ptr: *anyopaque, after: u64, out: []MetaOp, deadline: zio.Timeout) anyerror!usize,

        // Replica registry (peer discovery).
        /// Record/refresh a replica's heartbeat (liveness + per-lineage watermarks).
        /// The implementation copies what it needs; `status` need not outlive the call.
        reportStatus: *const fn (ptr: *anyopaque, status: ReplicaStatus) anyerror!void,
        /// Pick a live replica able to donate a snapshot of (`index_name`,
        /// `generation`) that a reader at `after` can resume from (file_version >=
        /// `after`), or null if none. The returned addr is allocated in `arena`.
        findDonor: *const fn (ptr: *anyopaque, arena: std.mem.Allocator, index_name: []const u8, generation: u64, after: u64) anyerror!?DonorInfo,

        /// Retention control: seqs <= `floor` are considered dropped for the lineage, so
        /// a `read(after < floor)` returns error.BelowRetention. The PG impl derives this
        /// from real retention; the stub takes it explicitly (admin/tests).
        setRetentionFloor: *const fn (ptr: *anyopaque, index_name: []const u8, generation: u64, floor: u64) anyerror!void,
    };

    pub fn append(self: Coordinator, index_name: []const u8, generation: u64, changes: []const Change, expected: ?u64) !u64 {
        return self.vtable.append(self.ptr, index_name, generation, changes, expected);
    }

    pub fn read(self: Coordinator, index_name: []const u8, generation: u64, after: u64, out: []Entry, deadline: zio.Timeout) !usize {
        return self.vtable.read(self.ptr, index_name, generation, after, out, deadline);
    }

    pub fn createIndex(self: Coordinator, name: []const u8) !u64 {
        return self.vtable.createIndex(self.ptr, name);
    }

    pub fn deleteIndex(self: Coordinator, name: []const u8) !u64 {
        return self.vtable.deleteIndex(self.ptr, name);
    }

    pub fn readMeta(self: Coordinator, after: u64, out: []MetaOp, deadline: zio.Timeout) !usize {
        return self.vtable.readMeta(self.ptr, after, out, deadline);
    }

    pub fn reportStatus(self: Coordinator, status: ReplicaStatus) !void {
        return self.vtable.reportStatus(self.ptr, status);
    }

    pub fn findDonor(self: Coordinator, arena: std.mem.Allocator, index_name: []const u8, generation: u64, after: u64) !?DonorInfo {
        return self.vtable.findDonor(self.ptr, arena, index_name, generation, after);
    }

    pub fn setRetentionFloor(self: Coordinator, index_name: []const u8, generation: u64, floor: u64) !void {
        return self.vtable.setRetentionFloor(self.ptr, index_name, generation, floor);
    }
};

/// In-memory changelog stub. Upholds the invariants above; not durable, not
/// partitioned, no retention — those live only in the PG-backed implementation.
pub const MemoryCoordinator = struct {
    allocator: std.mem.Allocator,
    mutex: zio.Mutex = .init,
    cond: zio.Condition = .init,
    rows: std.ArrayListUnmanaged(Row) = .empty,
    // Meta feed (index registry), global, never truncated.
    meta_ops: std.ArrayListUnmanaged(MetaEntry) = .empty,
    next_meta_pos: u64 = 1,
    // Replica registry (peer discovery), keyed by replica_id.
    replicas: std.ArrayListUnmanaged(ReplicaRecord) = .empty,
    // Simulated changelog retention per lineage: seqs <= floor are conceptually
    // dropped. The stub never truncates on its own (setRetentionFloor drives it in
    // tests); the PG impl computes this from real retention.
    retention: std.ArrayListUnmanaged(RetentionFloor) = .empty,

    // A replica whose heartbeat is older than this is treated as dead.
    const liveness_timeout: zio.Duration = .fromMilliseconds(30_000);

    const ReplicaRecord = struct {
        replica_id: []const u8, // owned
        advertise_addr: []const u8, // owned
        lineages: []OwnedLineage, // owned
        last_seen: zio.Timestamp,
    };
    const OwnedLineage = struct {
        index_name: []const u8, // owned
        generation: u64,
        applied: u64,
        file_version: u64,
    };
    const RetentionFloor = struct {
        index_name: []const u8, // owned
        generation: u64,
        floor: u64,
    };

    const Row = struct {
        index_name: []const u8, // owned
        generation: u64,
        seq: u64, // per-lineage, starts at 1
        change: Change, // insert hashes owned
    };

    const MetaEntry = struct {
        pos: u64,
        kind: MetaOp.Kind,
        index_name: []const u8, // owned
    };

    pub fn init(allocator: std.mem.Allocator) MemoryCoordinator {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *MemoryCoordinator) void {
        for (self.rows.items) |*row| self.freeRow(row);
        self.rows.deinit(self.allocator);
        for (self.meta_ops.items) |op| self.allocator.free(op.index_name);
        self.meta_ops.deinit(self.allocator);
        for (self.replicas.items) |*rec| self.freeReplicaRecord(rec);
        self.replicas.deinit(self.allocator);
        for (self.retention.items) |r| self.allocator.free(r.index_name);
        self.retention.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn coordinator(self: *MemoryCoordinator) Coordinator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable: Coordinator.VTable = .{
        .append = appendImpl,
        .read = readImpl,
        .createIndex = createIndexImpl,
        .deleteIndex = deleteIndexImpl,
        .readMeta = readMetaImpl,
        .reportStatus = reportStatusImpl,
        .findDonor = findDonorImpl,
        .setRetentionFloor = setRetentionFloorImpl,
    };

    fn setRetentionFloorImpl(ptr: *anyopaque, index_name: []const u8, generation: u64, floor: u64) anyerror!void {
        const self: *MemoryCoordinator = @ptrCast(@alignCast(ptr));
        return self.setRetentionFloor(index_name, generation, floor);
    }

    fn appendImpl(ptr: *anyopaque, index_name: []const u8, generation: u64, changes: []const Change, expected: ?u64) anyerror!u64 {
        const self: *MemoryCoordinator = @ptrCast(@alignCast(ptr));
        try self.mutex.lock();
        defer self.mutex.unlock();

        // Optimistic concurrency: check before mutating anything.
        var seq = self.maxSeqForLocked(index_name, generation);
        if (expected) |exp| {
            if (seq != exp) return error.VersionMismatch;
        }
        if (changes.len == 0) return seq;

        // Commit all rows atomically: on any failure, roll back to the entry state.
        const start_len = self.rows.items.len;
        errdefer {
            for (self.rows.items[start_len..]) |*row| self.freeRow(row);
            self.rows.shrinkRetainingCapacity(start_len);
        }

        for (changes) |change| {
            const name_copy = try self.allocator.dupe(u8, index_name);
            errdefer self.allocator.free(name_copy);
            const change_copy = try dupeChange(self.allocator, change);
            errdefer freeChange(self.allocator, change_copy);
            seq += 1;
            try self.rows.append(self.allocator, .{ .index_name = name_copy, .generation = generation, .seq = seq, .change = change_copy });
        }

        self.cond.broadcast();
        return seq;
    }

    fn readImpl(ptr: *anyopaque, index_name: []const u8, generation: u64, after: u64, out: []Entry, deadline: zio.Timeout) anyerror!usize {
        const self: *MemoryCoordinator = @ptrCast(@alignCast(ptr));
        try self.mutex.lock();
        defer self.mutex.unlock();

        // The reader wants seqs > after, but retention has dropped everything <= floor;
        // if after < floor those seqs are gone, so the reader must bootstrap.
        if (after < self.retentionFloorLocked(index_name, generation)) return error.BelowRetention;

        while (true) {
            var n: usize = 0;
            for (self.rows.items) |row| { // stored in append order == per-lineage seq order
                if (n == out.len) break;
                if (row.generation != generation) continue;
                if (!std.mem.eql(u8, row.index_name, index_name)) continue;
                if (row.seq <= after) continue;
                out[n] = .{ .id = row.seq, .change = row.change };
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

    fn maxSeqForLocked(self: *MemoryCoordinator, index_name: []const u8, generation: u64) u64 {
        var max: u64 = 0;
        for (self.rows.items) |row| {
            if (row.generation == generation and row.seq > max and std.mem.eql(u8, row.index_name, index_name)) max = row.seq;
        }
        return max;
    }

    fn freeRow(self: *MemoryCoordinator, row: *Row) void {
        self.allocator.free(row.index_name);
        freeChange(self.allocator, row.change);
    }

    fn reportStatusImpl(ptr: *anyopaque, status: ReplicaStatus) anyerror!void {
        const self: *MemoryCoordinator = @ptrCast(@alignCast(ptr));
        try self.mutex.lock();
        defer self.mutex.unlock();

        // Build the owned replacement first, so a mid-build failure leaves the old
        // record intact.
        var rec = try self.dupeReplicaRecord(status);
        errdefer self.freeReplicaRecord(&rec);

        for (self.replicas.items) |*existing| {
            if (std.mem.eql(u8, existing.replica_id, status.replica_id)) {
                self.freeReplicaRecord(existing);
                existing.* = rec;
                return;
            }
        }
        try self.replicas.append(self.allocator, rec);
    }

    fn findDonorImpl(ptr: *anyopaque, arena: std.mem.Allocator, index_name: []const u8, generation: u64, after: u64) anyerror!?DonorInfo {
        const self: *MemoryCoordinator = @ptrCast(@alignCast(ptr));
        try self.mutex.lock();
        defer self.mutex.unlock();

        var best_addr: ?[]const u8 = null;
        var best_fv: u64 = 0;
        for (self.replicas.items) |rec| {
            if (rec.last_seen.untilNow(.monotonic).toNanoseconds() > liveness_timeout.toNanoseconds()) continue; // dead
            for (rec.lineages) |ls| {
                if (ls.generation != generation) continue;
                if (!std.mem.eql(u8, ls.index_name, index_name)) continue;
                if (ls.file_version < after) continue; // a reader at `after` can't resume from here
                if (best_addr == null or ls.file_version > best_fv) {
                    best_addr = rec.advertise_addr;
                    best_fv = ls.file_version;
                }
            }
        }
        const addr = best_addr orelse return null;
        // Copy the addr into the caller's arena — the record may change after unlock.
        return .{ .advertise_addr = try arena.dupe(u8, addr), .file_version = best_fv };
    }

    fn dupeReplicaRecord(self: *MemoryCoordinator, status: ReplicaStatus) !ReplicaRecord {
        const id = try self.allocator.dupe(u8, status.replica_id);
        errdefer self.allocator.free(id);
        const addr = try self.allocator.dupe(u8, status.advertise_addr);
        errdefer self.allocator.free(addr);
        const lineages = try self.allocator.alloc(OwnedLineage, status.lineages.len);
        var n: usize = 0;
        errdefer {
            for (lineages[0..n]) |l| self.allocator.free(l.index_name);
            self.allocator.free(lineages);
        }
        for (status.lineages) |ls| {
            lineages[n] = .{
                .index_name = try self.allocator.dupe(u8, ls.index_name),
                .generation = ls.generation,
                .applied = ls.applied,
                .file_version = ls.file_version,
            };
            n += 1;
        }
        return .{ .replica_id = id, .advertise_addr = addr, .lineages = lineages, .last_seen = zio.Timestamp.now(.monotonic) };
    }

    fn freeReplicaRecord(self: *MemoryCoordinator, rec: *ReplicaRecord) void {
        self.allocator.free(rec.replica_id);
        self.allocator.free(rec.advertise_addr);
        for (rec.lineages) |l| self.allocator.free(l.index_name);
        self.allocator.free(rec.lineages);
    }

    fn retentionFloorLocked(self: *MemoryCoordinator, index_name: []const u8, generation: u64) u64 {
        for (self.retention.items) |r| {
            if (r.generation == generation and std.mem.eql(u8, r.index_name, index_name)) return r.floor;
        }
        return 0;
    }

    // Test scaffolding: simulate the changelog having dropped seqs <= `floor` for a
    // lineage (the PG impl derives this from real retention).
    pub fn setRetentionFloor(self: *MemoryCoordinator, index_name: []const u8, generation: u64, floor: u64) !void {
        try self.mutex.lock();
        defer self.mutex.unlock();
        for (self.retention.items) |*r| {
            if (r.generation == generation and std.mem.eql(u8, r.index_name, index_name)) {
                r.floor = floor;
                return;
            }
        }
        const name_copy = try self.allocator.dupe(u8, index_name);
        errdefer self.allocator.free(name_copy);
        try self.retention.append(self.allocator, .{ .index_name = name_copy, .generation = generation, .floor = floor });
    }

    // The generation of `name` if it's currently active (its latest meta op is a
    // create), else null.
    fn currentGenerationLocked(self: *MemoryCoordinator, name: []const u8) ?u64 {
        var gen: ?u64 = null;
        for (self.meta_ops.items) |op| {
            if (!std.mem.eql(u8, op.index_name, name)) continue;
            gen = if (op.kind == .create) op.pos else null;
        }
        return gen;
    }

    fn createIndexImpl(ptr: *anyopaque, name: []const u8) anyerror!u64 {
        const self: *MemoryCoordinator = @ptrCast(@alignCast(ptr));
        try self.mutex.lock();
        defer self.mutex.unlock();
        if (self.currentGenerationLocked(name)) |gen| return gen; // already active
        const pos = self.next_meta_pos;
        const name_copy = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(name_copy);
        try self.meta_ops.append(self.allocator, .{ .pos = pos, .kind = .create, .index_name = name_copy });
        self.next_meta_pos += 1;
        self.cond.broadcast();
        return pos; // generation == the create's meta pos
    }

    fn deleteIndexImpl(ptr: *anyopaque, name: []const u8) anyerror!u64 {
        const self: *MemoryCoordinator = @ptrCast(@alignCast(ptr));
        try self.mutex.lock();
        defer self.mutex.unlock();
        if (self.currentGenerationLocked(name) == null) return self.next_meta_pos - 1; // no-op
        const pos = self.next_meta_pos;
        const name_copy = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(name_copy);
        try self.meta_ops.append(self.allocator, .{ .pos = pos, .kind = .delete, .index_name = name_copy });
        self.next_meta_pos += 1;
        self.cond.broadcast();
        return pos;
    }

    fn readMetaImpl(ptr: *anyopaque, after: u64, out: []MetaOp, deadline: zio.Timeout) anyerror!usize {
        const self: *MemoryCoordinator = @ptrCast(@alignCast(ptr));
        try self.mutex.lock();
        defer self.mutex.unlock();
        while (true) {
            var n: usize = 0;
            for (self.meta_ops.items) |op| { // stored in pos order
                if (n == out.len) break;
                if (op.pos <= after) continue;
                out[n] = .{ .pos = op.pos, .kind = op.kind, .index_name = op.index_name };
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
};

fn dupeChange(allocator: std.mem.Allocator, change: Change) !Change {
    return switch (change) {
        .insert => |i| .{ .insert = .{ .id = i.id, .hashes = try allocator.dupe(u32, i.hashes) } },
        .delete => |d| .{ .delete = d },
        .set_metadata => |sm| blk: {
            const out = try allocator.alloc(MetadataEntry, sm.entries.len);
            var n: usize = 0;
            errdefer {
                for (out[0..n]) |e| {
                    allocator.free(e.key);
                    allocator.free(e.value);
                }
                allocator.free(out);
            }
            for (sm.entries) |e| {
                const k = try allocator.dupe(u8, e.key);
                errdefer allocator.free(k);
                out[n] = .{ .key = k, .value = try allocator.dupe(u8, e.value) };
                n += 1;
            }
            break :blk .{ .set_metadata = .{ .entries = out } };
        },
    };
}

fn freeChange(allocator: std.mem.Allocator, change: Change) void {
    switch (change) {
        .insert => |i| allocator.free(i.hashes),
        .delete => {},
        .set_metadata => |sm| {
            for (sm.entries) |e| {
                allocator.free(e.key);
                allocator.free(e.value);
            }
            allocator.free(sm.entries);
        },
    }
}

// ---- tests ----

const testing = std.testing;

fn ins(id: u32, hashes: []const u32) Change {
    return .{ .insert = .{ .id = id, .hashes = hashes } };
}

test "MemoryCoordinator: per-lineage sequences start at 1, one per op" {
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var cl = MemoryCoordinator.init(testing.allocator);
    defer cl.deinit();
    const co = cl.coordinator();

    // (main, gen 1): two ops -> seqs 1,2 -> version 2.
    try testing.expectEqual(@as(u64, 2), try co.append("main", 1, &.{ ins(1, &.{ 10, 20 }), ins(2, &.{30}) }, null));
    // A different lineage has its OWN sequence: first op is seq 1 (not 3). Same
    // name, higher generation (recreate).
    try testing.expectEqual(@as(u64, 1), try co.append("main", 5, &.{ins(9, &.{40})}, null));
    // Back to (main, gen 1): seq 3.
    try testing.expectEqual(@as(u64, 3), try co.append("main", 1, &.{.{ .delete = .{ .id = 1 } }}, null));
}

test "MemoryCoordinator: optimistic concurrency per lineage" {
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var cl = MemoryCoordinator.init(testing.allocator);
    defer cl.deinit();
    const co = cl.coordinator();

    // Fresh lineage: expected version is 0.
    try testing.expectError(error.VersionMismatch, co.append("a", 1, &.{ins(1, &.{10})}, 5));
    try testing.expectEqual(@as(u64, 1), try co.append("a", 1, &.{ins(1, &.{10})}, 0));
    // Now at 1; a stale expected fails and appends nothing.
    try testing.expectError(error.VersionMismatch, co.append("a", 1, &.{ins(2, &.{20})}, 0));
    try testing.expectEqual(@as(u64, 2), try co.append("a", 1, &.{ins(2, &.{20})}, 1));
}

test "MemoryCoordinator: read fills buffer in seq order, scoped by (name, generation)" {
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var cl = MemoryCoordinator.init(testing.allocator);
    defer cl.deinit();
    const co = cl.coordinator();

    _ = try co.append("a", 1, &.{ ins(1, &.{10}), ins(2, &.{20}) }, null); // (a,1): seqs 1,2
    _ = try co.append("b", 1, &.{ins(3, &.{30})}, null); // (b,1): seq 1 (other name)
    _ = try co.append("a", 1, &.{ins(4, &.{40})}, null); // (a,1): seq 3

    var buf: [8]Entry = undefined;
    // All of (a,1) from the start: seqs 1,2,3 (b's op is invisible).
    const n = try co.read("a", 1, 0, &buf, .{ .duration = .fromMilliseconds(0) });
    try testing.expectEqual(@as(usize, 3), n);
    try testing.expectEqual(@as(u64, 1), buf[0].id);
    try testing.expectEqual(@as(u64, 2), buf[1].id);
    try testing.expectEqual(@as(u64, 3), buf[2].id);
    try testing.expectEqual(@as(u32, 40), buf[2].change.insert.hashes[0]);

    // Buffer smaller than available -> capped at out.len, still seq order.
    var small: [2]Entry = undefined;
    try testing.expectEqual(@as(usize, 2), try co.read("a", 1, 0, &small, .{ .duration = .fromMilliseconds(0) }));
    try testing.expectEqual(@as(u64, 1), small[0].id);

    // After a position: only seqs > 2 for (a,1) -> just seq 3.
    try testing.expectEqual(@as(usize, 1), try co.read("a", 1, 2, &buf, .{ .duration = .fromMilliseconds(0) }));
    try testing.expectEqual(@as(u64, 3), buf[0].id);
}

test "MemoryCoordinator: read on empty times out, returns 0" {
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var cl = MemoryCoordinator.init(testing.allocator);
    defer cl.deinit();
    const co = cl.coordinator();

    var buf: [4]Entry = undefined;
    try testing.expectEqual(@as(usize, 0), try co.read("a", 1, 0, &buf, .{ .duration = .fromMilliseconds(5) }));
}

test "MemoryCoordinator: read blocks until a concurrent append wakes it" {
    const rt = try zio.Runtime.init(testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();

    var cl = MemoryCoordinator.init(testing.allocator);
    defer cl.deinit();
    const co = cl.coordinator();

    const Ctx = struct {
        fn reader(c: Coordinator, got: *usize, first: *u64) !void {
            var buf: [4]Entry = undefined;
            const n = try c.read("a", 1, 0, &buf, .none); // blocks until the append
            got.* = n;
            if (n > 0) first.* = buf[0].id;
        }
        fn appender(c: Coordinator) !void {
            _ = try c.append("a", 1, &.{ins(1, &.{ 10, 20 })}, null);
        }
    };

    var got: usize = 0;
    var first: u64 = 0;
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Ctx.reader, .{ co, &got, &first });
    try group.spawn(Ctx.appender, .{co});
    try group.wait();

    try testing.expectEqual(@as(usize, 1), got);
    try testing.expectEqual(@as(u64, 1), first);
}

test "MemoryCoordinator: meta feed create/delete/create, distinct generations, isolated data" {
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var cl = MemoryCoordinator.init(testing.allocator);
    defer cl.deinit();
    const co = cl.coordinator();

    const g1 = try co.createIndex("main");
    try testing.expectEqual(g1, try co.createIndex("main")); // idempotent while active

    _ = try co.append("main", g1, &.{ins(1, &.{10})}, null); // old lineage data: (main,g1,seq1)

    const del_pos = try co.deleteIndex("main");
    const g2 = try co.createIndex("main");
    try testing.expect(g2 != g1); // new lineage, higher generation
    try testing.expect(g2 > del_pos); // create pos is after the delete pos

    // The new lineage's data feed is empty and starts fresh at seq 1 — the old
    // lineage's op is invisible (isolation is the generation scope, no start_position).
    var buf: [8]Entry = undefined;
    try testing.expectEqual(@as(usize, 0), try co.read("main", g2, 0, &buf, .{ .duration = .fromMilliseconds(0) }));
    try testing.expectEqual(@as(u64, 1), try co.append("main", g2, &.{ins(2, &.{20})}, null));

    // Meta feed replays the lifecycle in pos order.
    var meta: [8]MetaOp = undefined;
    const n = try co.readMeta(0, &meta, .{ .duration = .fromMilliseconds(0) });
    try testing.expectEqual(@as(usize, 3), n);
    try testing.expectEqual(MetaOp.Kind.create, meta[0].kind);
    try testing.expectEqual(g1, meta[0].pos);
    try testing.expectEqual(MetaOp.Kind.delete, meta[1].kind);
    try testing.expectEqual(MetaOp.Kind.create, meta[2].kind);
    try testing.expectEqual(g2, meta[2].pos);

    // A different name shares the meta pos sequence, so its generation is higher still.
    const other = try co.createIndex("other");
    try testing.expect(other > g2);

    // Deleting a name that isn't active is a no-op: returns the latest meta pos.
    try testing.expectEqual(other, try co.deleteIndex("does_not_exist"));
}

test "MemoryCoordinator: findDonor picks a live replica that can serve the reader" {
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var cl = MemoryCoordinator.init(testing.allocator);
    defer cl.deinit();
    const co = cl.coordinator();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // No replicas yet -> no donor.
    try testing.expect((try co.findDonor(a, "main", 1, 0)) == null);

    try co.reportStatus(.{ .replica_id = "r1", .advertise_addr = "http://r1", .lineages = &.{
        .{ .index_name = "main", .generation = 1, .applied = 10, .file_version = 5 },
    } });
    try co.reportStatus(.{ .replica_id = "r2", .advertise_addr = "http://r2", .lineages = &.{
        .{ .index_name = "main", .generation = 1, .applied = 20, .file_version = 15 },
        .{ .index_name = "other", .generation = 3, .applied = 4, .file_version = 4 },
    } });

    // A reader at after=0: both qualify, prefer the higher checkpoint watermark (r2).
    const d = (try co.findDonor(a, "main", 1, 0)).?;
    try testing.expectEqualStrings("http://r2", d.advertise_addr);
    try testing.expectEqual(@as(u64, 15), d.file_version);

    // A reader past r1's watermark (after=10): only r2 (fv 15) can resume it.
    try testing.expectEqualStrings("http://r2", (try co.findDonor(a, "main", 1, 10)).?.advertise_addr);
    // Past everyone (after=20): no donor.
    try testing.expect((try co.findDonor(a, "main", 1, 20)) == null);
    // Wrong generation / name -> no donor.
    try testing.expect((try co.findDonor(a, "main", 2, 0)) == null);
    try testing.expect((try co.findDonor(a, "nope", 1, 0)) == null);

    // A replica re-reporting replaces its prior status (no leak; new watermark wins).
    try co.reportStatus(.{ .replica_id = "r1", .advertise_addr = "http://r1b", .lineages = &.{
        .{ .index_name = "main", .generation = 1, .applied = 40, .file_version = 30 },
    } });
    const d3 = (try co.findDonor(a, "main", 1, 20)).?;
    try testing.expectEqualStrings("http://r1b", d3.advertise_addr);
    try testing.expectEqual(@as(u64, 30), d3.file_version);
}

test "MemoryCoordinator: read below the retention floor signals bootstrap" {
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var cl = MemoryCoordinator.init(testing.allocator);
    defer cl.deinit();
    const co = cl.coordinator();

    _ = try co.append("main", 1, &.{ ins(1, &.{10}), ins(2, &.{20}), ins(3, &.{30}) }, null);
    try cl.setRetentionFloor("main", 1, 2); // seqs <= 2 conceptually dropped

    var buf: [8]Entry = undefined;
    const zero: zio.Timeout = .{ .duration = .fromMilliseconds(0) };
    // A reader below the floor wants seqs that are gone -> must bootstrap.
    try testing.expectError(error.BelowRetention, co.read("main", 1, 0, &buf, zero));
    try testing.expectError(error.BelowRetention, co.read("main", 1, 1, &buf, zero));
    // At the floor: the next wanted seq (3) is retained -> OK.
    try testing.expectEqual(@as(usize, 1), try co.read("main", 1, 2, &buf, zero));
    try testing.expectEqual(@as(u64, 3), buf[0].id);
    // Another lineage is unaffected (its floor is 0).
    _ = try co.append("main", 5, &.{ins(9, &.{40})}, null);
    try testing.expectEqual(@as(usize, 1), try co.read("main", 5, 0, &buf, zero));
}
