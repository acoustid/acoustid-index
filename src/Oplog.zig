const std = @import("std");
const assert = std.debug.assert;

const common = @import("common.zig");
const Change = common.Change;

const Self = @This();

pub const FileInfo = struct {
    id: u64 = 0,

    fn cmp(_: void, a: FileInfo, b: FileInfo) bool {
        return a.id < b.id;
    }
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

files: std.ArrayList(FileInfo),

current_file: ?std.fs.File = null,
current_file_size: usize = 0,
max_file_size: usize = 1_000_000,

last_commit_id: u64 = 0,

pub fn init(allocator: std.mem.Allocator, dir: std.fs.Dir) Self {
    return Self{
        .allocator = allocator,
        .dir = dir,
        .files = std.ArrayList(FileInfo).init(allocator),
    };
}

pub fn deinit(self: *Self) void {
    self.closeCurrentFile();
    self.files.deinit();
}

pub fn open(self: *Self, first_commit_id: u64) !void {
    try self.dir.makePath(".");

    var it = self.dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind == .File) {
            const id = try parseFileName(entry.name);
            try self.files.append(.{ .id = id });
        }
    }

    std.sort.pdq(FileInfo, self.files.items, {}, FileInfo.cmp);

    try self.truncate(first_commit_id);

    for (self.files.items) |file_info| {
        const commit_ids = self.scanFile(file_info.id) catch |err| {
            if (err == error.NoCommits) {
                continue;
            }
            return err;
        };
        self.last_commit_id = @max(self.last_commit_id, commit_ids.last);
    }
}

fn scanFile(self: *Self, file_id: u64) !struct { first: u64, last: u64 } {
    var buf: [file_name_size]u8 = undefined;
    const file_name = try generateFileName(&buf, file_id);

    var file = try self.dir.openFile(file_name, .{});
    defer file.close();

    var buffered_reader = std.io.bufferedReader(file.reader());
    var reader = buffered_reader.reader();

    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena;

    var allocator = arena.allocator();

    var line_buf = try allocator.alloc(u8, 64 * 1024);

    var first_commit_id: ?u64 = null;
    var last_commit_id: ?u64 = null;

    while (true) {
        const line = reader.readUntilDelimiter(&line_buf, '\n') catch |err| {
            if (err == error.EndOfStream) break;
            return err;
        };
        if (line.len == 0) continue;

        var parsed_entry = try std.json.parseFromSlice(Entry, &allocator, line, .{});
        defer parsed_entry.deinit();

        if (parsed_entry.value.commit != null) {
            if (first_commit_id == null) {
                first_commit_id = parsed_entry.value.id;
            }
            last_commit_id = parsed_entry.value.id;
        }
    }

    if (first_commit_id == null or last_commit_id == null) {
        return error.NoCommits;
    }

    return .{ .first = first_commit_id.?, .last = last_commit_id.? };
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
    const file = try self.dir.createFile(file_name, .{ .exclusive = true });
    return file;
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

pub fn truncate(self: *Self, commit_id: u64) void {
    assert(std.sort.isSorted(FileInfo, self.files.items, {}, FileInfo.cmp));

    var pos = std.sort.lowerBound(FileInfo, self.files.items, FileInfo{ .id = commit_id }, {}, FileInfo.cmp);
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

pub fn write(self: *Self, changes: []const Change) !u64 {
    const commit_id = self.last_commit_id + 1;

    const file = try self.getFile(commit_id);
    var bufferred_writer = std.io.bufferedWriter(file.writer());
    const writer = bufferred_writer.writer();

    try writeEntries(writer, commit_id, changes);

    try bufferred_writer.flush();

    try file.sync();

    self.current_file_size += changes.len;
    self.last_commit_id = commit_id;
    return commit_id;
}

test "write entries" {
    var tmpDir = std.testing.tmpDir(.{});
    defer tmpDir.cleanup();

    var oplog = Self.init(std.testing.allocator, tmpDir.dir);
    defer oplog.deinit();

    const changes = [_]Change{.{ .insert = .{
        .id = 1,
        .hashes = &[_]u32{ 1, 2, 3 },
    } }};

    const commit_id = try oplog.write(&changes);
    try std.testing.expectEqual(1, commit_id);

    var file = try tmpDir.dir.openFile("0000000000000001.xlog", .{});
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
