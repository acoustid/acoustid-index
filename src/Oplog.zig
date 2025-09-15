const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.oplog);

const msgpack = @import("msgpack");

const Change = @import("change.zig").Change;
const Metadata = @import("change.zig").Metadata;
const Transaction = @import("change.zig").Transaction;

const Self = @This();

pub const WriteOptions = struct {
    expected_last_version: ?u64 = null,
    version: ?u64 = null,
};

pub const FileInfo = struct {
    id: u64 = 0,

    fn lessThan(_: void, a: FileInfo, b: FileInfo) bool {
        return a.id < b.id;
    }

    fn order(a: FileInfo, b: FileInfo) std.math.Order {
        return std.math.order(a.id, b.id);
    }
};

allocator: std.mem.Allocator,
dir: std.fs.Dir,

write_lock: std.Thread.Mutex = .{},

files: std.ArrayList(FileInfo),

current_file: ?std.fs.File = null,
current_file_size: usize = 0,
max_file_size: usize = 16 * 1024 * 1024,

last_commit_id: u64 = 0,

pub fn init(allocator: std.mem.Allocator, parent_dir: std.fs.Dir) !Self {
    var dir = try parent_dir.makeOpenPath("oplog", .{ .iterate = true });
    errdefer dir.close();

    return Self{
        .allocator = allocator,
        .dir = dir,
        .files = std.ArrayList(FileInfo).init(allocator),
    };
}

pub fn deinit(self: *Self) void {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    log.info("closing oplog", .{});

    self.closeCurrentFile();

    self.files.deinit();

    self.dir.close();
}

pub fn open(self: *Self, first_commit_id: u64, receiver: anytype, ctx: anytype) !void {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    log.info("opening oplog", .{});

    var it = self.dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind == .file) {
            if (std.mem.endsWith(u8, entry.name, ".xlog")) {
                const id = try parseFileName(entry.name);
                try self.files.append(.{ .id = id });
            }
        }
    }

    std.sort.pdq(FileInfo, self.files.items, {}, FileInfo.lessThan);

    try self.truncateNoLock(first_commit_id);

    var max_commit_id: u64 = 0;
    var oplog_it = OplogIterator.init(self.allocator, self.dir, self.files, first_commit_id);
    defer oplog_it.deinit();
    while (try oplog_it.next()) |txn| {
        max_commit_id = @max(max_commit_id, txn.id);
        _ = try receiver(ctx, txn.changes, txn.metadata, txn.id);
    }
    self.last_commit_id = max_commit_id;
}

fn parseFileName(file_name: []const u8) !u64 {
    if (file_name.len != file_name_size) {
        return error.InvalidFileName;
    }
    if (!std.mem.endsWith(u8, file_name, ".xlog")) {
        return error.InvalidFileName;
    }
    return std.fmt.parseUnsigned(u64, file_name[0..16], 16) catch {
        return error.InvalidFileName;
    };
}

test "parseFileName" {
    const testing = std.testing;

    // Test case 1: Valid file name
    try testing.expectEqual(@as(u64, 0x123456789abcdef0), try parseFileName("123456789abcdef0.xlog"));

    // Test case 2: Invalid length
    try testing.expectError(error.InvalidFileName, parseFileName("123456789abcdef.xlog"));
    try testing.expectError(error.InvalidFileName, parseFileName("123456789abcdef01.xlog"));

    // Test case 3: Invalid extension
    try testing.expectError(error.InvalidFileName, parseFileName("123456789abcdef0.txt"));

    // Test case 4: Invalid hexadecimal
    try testing.expectError(error.InvalidFileName, parseFileName("123456789abcdefg.xlog"));

    // Test case 5: Minimum value
    try testing.expectEqual(@as(u64, 0), try parseFileName("0000000000000000.xlog"));

    // Test case 6: Maximum value
    try testing.expectEqual(@as(u64, std.math.maxInt(u64)), try parseFileName("ffffffffffffffff.xlog"));
}

fn generateFileName(buf: []u8, commit_id: u64) ![]u8 {
    return std.fmt.bufPrint(buf, "{x:0>16}.xlog", .{commit_id});
}

test "generateFileName" {
    const testing = std.testing;
    var buf: [file_name_size]u8 = undefined;

    // Test case 1: Minimum commit ID
    try testing.expectEqualStrings("0000000000000000.xlog", try generateFileName(&buf, 0));

    // Test case 2: Maximum commit ID
    try testing.expectEqualStrings("ffffffffffffffff.xlog", try generateFileName(&buf, std.math.maxInt(u64)));

    // Test case 3: Random commit ID
    try testing.expectEqualStrings("123456789abcdef0.xlog", try generateFileName(&buf, 0x123456789abcdef0));

    // Test case 4: Buffer too small
    var small_buf: [file_name_size - 1]u8 = undefined;
    try testing.expectError(error.NoSpaceLeft, generateFileName(&small_buf, 0));
}

const file_name_size = 16 + 1 + 4;

fn openFile(self: *Self, commit_id: u64) !std.fs.File {
    var buf: [file_name_size]u8 = undefined;
    const file_name = try generateFileName(&buf, commit_id);
    log.info("creating oplog file {s}", .{file_name});
    return self.dir.createFile(file_name, .{ .exclusive = true });
}

fn closeCurrentFile(self: *Self) void {
    if (self.current_file) |file| {
        file.close();
        self.current_file = null;
        self.current_file_size = 0;
    }
}

fn getFile(self: *Self, commit_id: u64) !std.fs.File {
    if (self.current_file) |file| {
        if (self.current_file_size < self.max_file_size) {
            return file;
        }
        self.closeCurrentFile();
    }

    try self.files.append(.{ .id = commit_id });
    const file = self.openFile(commit_id) catch |err| {
        self.files.shrinkRetainingCapacity(self.files.items.len - 1);
        return err;
    };
    self.current_file = file;
    self.current_file_size = 0;
    return file;
}

fn truncateNoLock(self: *Self, commit_id: u64) !void {
    assert(std.sort.isSorted(FileInfo, self.files.items, {}, FileInfo.lessThan));

    var pos = std.sort.lowerBound(FileInfo, self.files.items, FileInfo{ .id = commit_id }, FileInfo.order);
    if (pos > 0) {
        pos -= 1;
    }

    var buf: [file_name_size]u8 = undefined;
    while (pos > 0) {
        pos -= 1;
        const file_info = self.files.items[pos];
        const file_name = try generateFileName(&buf, file_info.id);
        log.info("deleting oplog file {s}", .{file_name});
        try self.dir.deleteFile(file_name);
        _ = self.files.orderedRemove(pos);
    }
}

pub fn truncate(self: *Self, commit_id: u64) !void {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    try self.truncateNoLock(commit_id);
}

pub fn write(self: *Self, changes: []const Change, metadata: ?Metadata, options: WriteOptions) !u64 {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    // Validate expected version if provided
    if (options.expected_last_version) |expected| {
        const current_version = self.last_commit_id;
        if (current_version != expected) {
            return error.VersionMismatch;
        }
    }

    const commit_id = if (options.version) |custom_version| blk: {
        // Validate custom version is greater than current last_commit_id
        if (custom_version <= self.last_commit_id) {
            log.warn("Custom version {} is not greater than current last_commit_id {}", .{ custom_version, self.last_commit_id });
            return error.VersionNotMonotonic;
        }
        break :blk custom_version;
    } else self.last_commit_id + 1;

    const file = try self.getFile(commit_id);
    var counting_writer = std.io.countingWriter(file.writer());
    var bufferred_writer = std.io.bufferedWriter(counting_writer.writer());
    const writer = bufferred_writer.writer();

    try msgpack.encode(Transaction{
        .id = commit_id,
        .changes = changes,
        .metadata = metadata,
    }, writer);

    try bufferred_writer.flush();

    self.current_file_size += counting_writer.bytes_written;
    self.last_commit_id = commit_id;

    file.sync() catch |err| {
        if (err == error.InputOutput) {
            // FIXME: maybe we try to reload the oplog from disk, there is no other way to know what happened
            std.debug.panic("failed to sync oplog file: {s}", .{@errorName(err)});
        }
        return err;
    };

    return commit_id;
}

test "write entries" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var oplog = try Self.init(std.testing.allocator, tmp_dir.dir);
    defer oplog.deinit();

    const Updater = struct {
        pub fn receive(self: *@This(), changes: []const Change, metadata: ?Metadata, commit_id: u64) !void {
            _ = self;
            _ = changes;
            _ = metadata;
            _ = commit_id;
        }
    };

    var updater: Updater = .{};

    try oplog.open(0, Updater.receive, &updater);

    const changes = [_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }};

    _ = try oplog.write(&changes, null, .{});

    var file = try tmp_dir.dir.openFile("oplog/0000000000000001.xlog", .{});
    defer file.close();

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var unpacker = msgpack.unpacker(file.reader(), arena.allocator());
    const txn = try unpacker.read(Transaction);

    try std.testing.expectEqual(1, txn.id);
    try std.testing.expectEqualDeep(&changes, txn.changes);
}

pub const OplogIterator = struct {
    allocator: std.mem.Allocator,
    dir: std.fs.Dir,
    files: std.ArrayList(FileInfo),
    first_commit_id: u64,
    current_iterator: ?OplogFileIterator = null,
    current_file_index: usize = 0,

    pub fn init(allocator: std.mem.Allocator, dir: std.fs.Dir, files: std.ArrayList(FileInfo), first_commit_id: u64) OplogIterator {
        return OplogIterator{
            .allocator = allocator,
            .dir = dir,
            .files = files,
            .first_commit_id = first_commit_id,
        };
    }

    pub fn deinit(self: *OplogIterator) void {
        if (self.current_iterator) |*iterator| {
            iterator.deinit();
        }
    }

    pub fn next(self: *OplogIterator) !?Transaction {
        while (true) {
            if (self.current_iterator) |*iterator| {
                if (try iterator.next()) |entry| {
                    if (entry.id < self.first_commit_id) {
                        continue;
                    }
                    return entry;
                }
                iterator.deinit();
                self.current_iterator = null;
                self.current_file_index += 1;
            }
            if (self.current_file_index >= self.files.items.len) {
                return null;
            }
            var buf: [file_name_size]u8 = undefined;
            const file_name = try generateFileName(&buf, self.files.items[self.current_file_index].id);
            log.info("reading oplog file {s}", .{file_name});
            const file = try self.dir.openFile(file_name, .{});
            self.current_iterator = OplogFileIterator.init(self.allocator, file);
        }
        unreachable;
    }
};

pub const OplogFileIterator = struct {
    arena: std.heap.ArenaAllocator,
    file: std.fs.File,
    buffered_reader: std.io.BufferedReader(4096, std.fs.File.Reader),

    pub fn init(allocator: std.mem.Allocator, file: std.fs.File) OplogFileIterator {
        return OplogFileIterator{
            .arena = std.heap.ArenaAllocator.init(allocator),
            .file = file,
            .buffered_reader = std.io.bufferedReader(file.reader()),
        };
    }

    pub fn deinit(self: *OplogFileIterator) void {
        self.file.close();
        self.arena.deinit();
    }

    pub fn next(self: *OplogFileIterator) !?Transaction {
        _ = self.arena.reset(.retain_capacity);

        return msgpack.decodeLeaky(Transaction, self.arena.allocator(), self.buffered_reader.reader()) catch |err| {
            if (err == error.EndOfStream) {
                return null;
            }
            return err;
        };
    }
};

test "write with expected version validation" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var oplog = try Self.init(std.testing.allocator, tmp_dir.dir);
    defer oplog.deinit();

    const changes = [_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }};

    // First write should succeed (no expected version)
    const version1 = try oplog.write(&changes, null, .{});
    try std.testing.expectEqual(1, version1);
    try std.testing.expectEqual(1, oplog.last_commit_id);

    // Second write with correct expected version should succeed
    const version2 = try oplog.write(&changes, null, .{ .expected_last_version = 1 });
    try std.testing.expectEqual(2, version2);
    try std.testing.expectEqual(2, oplog.last_commit_id);

    // Write with wrong expected version should fail
    const result = oplog.write(&changes, null, .{ .expected_last_version = 1 });
    try std.testing.expectError(error.VersionMismatch, result);
    try std.testing.expectEqual(2, oplog.last_commit_id); // Should remain unchanged

    // Write with correct expected version should succeed after failed attempt
    const version3 = try oplog.write(&changes, null, .{ .expected_last_version = 2 });
    try std.testing.expectEqual(3, version3);
    try std.testing.expectEqual(3, oplog.last_commit_id);
}

test "write with custom version" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var oplog = try Self.init(std.testing.allocator, tmp_dir.dir);
    defer oplog.deinit();

    const changes = [_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }};

    // Write with custom version should use the provided version
    const version1 = try oplog.write(&changes, null, .{ .version = 100 });
    try std.testing.expectEqual(100, version1);
    try std.testing.expectEqual(100, oplog.last_commit_id);

    // Next custom version must be higher
    const version2 = try oplog.write(&changes, null, .{ .version = 200 });
    try std.testing.expectEqual(200, version2);
    try std.testing.expectEqual(200, oplog.last_commit_id);

    // Custom version that's not monotonic should fail
    const result = oplog.write(&changes, null, .{ .version = 150 });
    try std.testing.expectError(error.VersionNotMonotonic, result);
    try std.testing.expectEqual(200, oplog.last_commit_id); // Should remain unchanged

    // Mix custom and auto-increment
    const version3 = try oplog.write(&changes, null, .{});
    try std.testing.expectEqual(201, version3);
    try std.testing.expectEqual(201, oplog.last_commit_id);
}
