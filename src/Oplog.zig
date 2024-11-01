const std = @import("std");
const assert = std.debug.assert;

const common = @import("common.zig");
const Change = common.Change;
const InMemoryIndex = @import("InMemoryIndex.zig");

const Self = @This();

pub const FileInfo = struct {
    id: u64 = 0,

    fn cmp(_: void, a: FileInfo, b: FileInfo) bool {
        return a.id < b.id;
    }
};

pub const Transaction = struct {
    commit_id: u64,
    changes: []Change,
};

pub const Entry = struct {
    id: u64 = 0,
    apply: ?common.Change = null,
    begin: ?struct {
        size: u32 = 0,
    } = null,
    commit: ?bool = null,
};

allocator: std.mem.Allocator,
dir: std.fs.Dir,

lock_file: ?std.fs.File = null,

write_lock: std.Thread.Mutex = .{},

files: std.ArrayList(FileInfo),

current_file: ?std.fs.File = null,
current_file_size: usize = 0,
max_file_size: usize = 1_000_000,

next_commit_id: u64 = 1,

pub fn init(allocator: std.mem.Allocator, dir: std.fs.Dir) Self {
    return Self{
        .allocator = allocator,
        .dir = dir,
        .files = std.ArrayList(FileInfo).init(allocator),
    };
}

const lock_file_name = ".lock";

pub fn deinit(self: *Self) void {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    self.closeCurrentFile();

    self.files.deinit();

    if (self.lock_file) |file| {
        file.close();
        self.lock_file = null;
    }
    self.dir.deleteFile(lock_file_name) catch {};
}

pub fn open(self: *Self, first_commit_id: u64, index: *InMemoryIndex) !void {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    self.lock_file = try self.dir.createFile(lock_file_name, .{ .lock = .exclusive });

    var it = self.dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind == .file) {
            if (std.mem.endsWith(u8, entry.name, ".xlog")) {
                const id = try parseFileName(entry.name);
                try self.files.append(.{ .id = id });
            }
        }
    }

    std.sort.pdq(FileInfo, self.files.items, {}, FileInfo.cmp);

    try self.truncateNoLock(first_commit_id);

    var max_commit_id: u64 = 0;
    var oplog_it = OplogIterator.init(self.allocator, self.dir, self.files, first_commit_id);
    defer oplog_it.deinit();
    while (try oplog_it.next()) |txn| {
        max_commit_id = @max(max_commit_id, txn.commit_id);
        try index.update(txn.changes, txn.commit_id);
    }
    self.next_commit_id = max_commit_id + 1;
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
    std.log.info("creating oplog file {s}", .{file_name});
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
    assert(std.sort.isSorted(FileInfo, self.files.items, {}, FileInfo.cmp));

    var pos = std.sort.lowerBound(FileInfo, FileInfo{ .id = commit_id }, self.files.items, {}, FileInfo.cmp);
    if (pos > 0) {
        pos -= 1;
    }

    var buf: [file_name_size]u8 = undefined;
    while (pos > 0) {
        pos -= 1;
        const file_info = self.files.orderedRemove(pos);
        const file_name = try generateFileName(&buf, file_info.id);
        std.log.info("deleting oplog file {s}", .{file_name});
        try self.dir.deleteFile(file_name);
    }
}

pub fn truncate(self: *Self, commit_id: u64) !void {
    self.write_lock.lock();
    defer self.write_lock.unlock();

    try self.truncateNoLock(commit_id);
}

const newline: u8 = '\n';

fn writeEntries(writer: anytype, commit_id: u64, changes: []const Change) !void {
    const begin_entry = Entry{
        .id = commit_id,
        .begin = .{
            .size = @truncate(changes.len),
        },
    };
    try writer.writeByte(newline);
    try std.json.stringify(begin_entry, .{ .emit_null_optional_fields = false }, writer);

    for (changes) |change| {
        const entry = Entry{
            .id = commit_id,
            .apply = change,
        };
        try writer.writeByte(newline);
        try std.json.stringify(entry, .{ .emit_null_optional_fields = false }, writer);
    }

    const commit_entry = Entry{
        .id = commit_id,
        .commit = true,
    };
    try writer.writeByte(newline);
    try std.json.stringify(commit_entry, .{ .emit_null_optional_fields = false }, writer);
}

pub fn write(self: *Self, changes: []const Change, index: *InMemoryIndex) !void {
    var committed: bool = false;

    const update_id = try index.prepareUpdate(changes);
    defer {
        if (!committed) index.cancelUpdate(update_id);
    }

    self.write_lock.lock();
    defer self.write_lock.unlock();

    const commit_id = self.next_commit_id;

    const file = try self.getFile(commit_id);
    var bufferred_writer = std.io.bufferedWriter(file.writer());
    const writer = bufferred_writer.writer();

    try writeEntries(writer, commit_id, changes);

    try bufferred_writer.flush();

    try file.sync();

    self.current_file_size += changes.len;
    self.next_commit_id += 1;

    index.commitUpdate(update_id, commit_id);
    committed = true;
}

test "write entries" {
    var tmpDir = std.testing.tmpDir(.{});
    defer tmpDir.cleanup();

    var oplogDir = try tmpDir.dir.makeOpenPath("oplog", .{ .iterate = true });
    defer oplogDir.close();

    var oplog = Self.init(std.testing.allocator, oplogDir);
    defer oplog.deinit();

    var stage = InMemoryIndex.init(std.testing.allocator, .{});
    defer stage.deinit();

    try oplog.open(0, &stage);

    const changes = [_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }};

    try oplog.write(&changes, &stage);

    var file = try oplogDir.openFile("0000000000000001.xlog", .{});
    defer file.close();

    const contents = try file.reader().readAllAlloc(std.testing.allocator, 1024 * 1024);
    defer std.testing.allocator.free(contents);

    const expected =
        \\
        \\{"id":1,"begin":{"size":1}}
        \\{"id":1,"apply":{"insert":{"id":1,"hashes":[1,2,3]}}}
        \\{"id":1,"commit":true}
    ;
    try std.testing.expectEqualStrings(expected, contents);
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
                    if (entry.commit_id < self.first_commit_id) {
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

        var allocator = self.arena.allocator();
        var reader = self.buffered_reader.reader();

        var commit_id: u64 = 0;
        var changes = std.ArrayList(Change).init(allocator);

        const line_buf = try allocator.alloc(u8, 64 * 1024);
        defer allocator.free(line_buf);

        while (true) {
            const line = reader.readUntilDelimiter(line_buf, '\n') catch |err| {
                if (err == error.EndOfStream) {
                    break;
                }
                return err;
            };
            if (line.len == 0) {
                continue;
            }

            const entry = try std.json.parseFromSliceLeaky(Entry, allocator, line, .{});

            if (entry.begin) |begin| {
                commit_id = entry.id;
                changes.clearRetainingCapacity();
                try changes.ensureTotalCapacity(begin.size);
            }

            if (entry.apply) |apply| {
                if (entry.id == commit_id) {
                    try changes.append(apply);
                }
            }

            if (entry.commit) |_| {
                if (entry.id == commit_id) {
                    return Transaction{
                        .commit_id = commit_id,
                        .changes = changes.items,
                    };
                }
            }
        }

        return null;
    }
};
