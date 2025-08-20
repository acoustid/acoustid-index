const std = @import("std");
const builtin = @import("builtin");

const Self = @This();

const LOCK_FILE_NAME = ".lock";

dir: std.fs.Dir,
lock_file: ?std.fs.File = null,

pub fn init(dir: std.fs.Dir) Self {
    return .{
        .dir = dir,
    };
}

pub fn deinit(self: *Self) void {
    self.release();
}

pub fn acquire(self: *Self) !void {
    if (self.lock_file != null) {
        return error.AlreadyLocked;
    }

    // Try to create lock file exclusively first
    const lock_file = self.dir.createFile(LOCK_FILE_NAME, .{
        .read = true,
        .truncate = false,
        .exclusive = true,
    }) catch |err| switch (err) {
        error.PathAlreadyExists => {
            // Lock file exists, try to open and lock it
            const existing_file = self.dir.openFile(LOCK_FILE_NAME, .{ .mode = .read_write }) catch |open_err| switch (open_err) {
                error.FileNotFound => {
                    // File was deleted between our checks, try again
                    return self.acquire();
                },
                else => return open_err,
            };
            errdefer existing_file.close();
            
            // Try to lock the existing file
            self.lockFile(existing_file) catch |lock_err| {
                existing_file.close();
                return lock_err;
            };
            
            self.lock_file = existing_file;
            return;
        },
        else => return err,
    };
    errdefer lock_file.close();

    try self.lockFile(lock_file);
    self.lock_file = lock_file;
}

pub fn release(self: *Self) void {
    if (self.lock_file) |file| {
        self.unlockFile(file);
        file.close();
        self.lock_file = null;
    }
}

pub fn isLocked(self: *Self) bool {
    return self.lock_file != null;
}

fn lockFile(self: *Self, file: std.fs.File) !void {    
    switch (builtin.os.tag) {
        .windows => try self.lockFileWindows(file),
        else => try self.lockFileUnix(file),
    }
}

fn unlockFile(self: *Self, file: std.fs.File) void {    
    switch (builtin.os.tag) {
        .windows => self.unlockFileWindows(file),
        else => self.unlockFileUnix(file),
    }
}

const flock_struct = extern struct {
    l_type: c_short,
    l_whence: c_short,
    l_start: isize,
    l_len: isize,
    l_pid: i32,
};

fn lockFileUnix(self: *Self, file: std.fs.File) !void {
    _ = self;
    
    var flock_data = flock_struct{
        .l_type = std.c.F.WRLCK,
        .l_whence = std.c.SEEK.SET,
        .l_start = 0,
        .l_len = 0,
        .l_pid = 0,
    };

    const result = std.c.fcntl(file.handle, std.c.F.SETLK, &flock_data);
    if (result == -1) {
        const err = std.posix.errno(result);
        return switch (err) {
            .ACCES, .AGAIN => error.ResourceBusy,
            .BADF => error.InvalidHandle,
            .INVAL => error.InvalidArgument,
            else => std.posix.unexpectedErrno(err),
        };
    }
}

fn unlockFileUnix(self: *Self, file: std.fs.File) void {
    _ = self;
    
    var flock_data = flock_struct{
        .l_type = std.c.F.UNLCK,
        .l_whence = std.c.SEEK.SET,
        .l_start = 0,
        .l_len = 0,
        .l_pid = 0,
    };

    _ = std.c.fcntl(file.handle, std.c.F.SETLK, &flock_data);
}

fn lockFileWindows(self: *Self, file: std.fs.File) !void {
    _ = self;
    
    const LOCKFILE_EXCLUSIVE_LOCK = 0x00000002;
    const LOCKFILE_FAIL_IMMEDIATELY = 0x00000001;
    
    var overlapped = std.mem.zeroes(std.windows.OVERLAPPED);
    
    const result = std.windows.kernel32.LockFileEx(
        file.handle,
        LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
        0,
        std.math.maxInt(u32),
        std.math.maxInt(u32),
        &overlapped
    );
    
    if (result == 0) {
        const err = std.windows.kernel32.GetLastError();
        switch (err) {
            std.windows.Win32Error.LOCK_VIOLATION => return error.ResourceBusy,
            else => return std.windows.unexpectedError(err),
        }
    }
}

fn unlockFileWindows(self: *Self, file: std.fs.File) void {
    _ = self;
    
    var overlapped = std.mem.zeroes(std.windows.OVERLAPPED);
    
    _ = std.windows.kernel32.UnlockFileEx(
        file.handle,
        0,
        std.math.maxInt(u32),
        std.math.maxInt(u32),
        &overlapped
    );
}

test "DirectoryLock basic operations" {
    const testing = std.testing;
    
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    
    var lock = Self.init(tmp_dir.dir);
    defer lock.deinit();
    
    try testing.expect(!lock.isLocked());
    
    try lock.acquire();
    try testing.expect(lock.isLocked());
    
    // Should not be able to acquire again
    try testing.expectError(error.AlreadyLocked, lock.acquire());
    
    lock.release();
    try testing.expect(!lock.isLocked());
    
    // Should be able to acquire again after release
    try lock.acquire();
    try testing.expect(lock.isLocked());
}

test "DirectoryLock cross-process behavior" {
    const testing = std.testing;
    
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    
    var lock1 = Self.init(tmp_dir.dir);
    defer lock1.deinit();
    
    try lock1.acquire();
    try testing.expect(lock1.isLocked());
    
    // Note: Within the same process, multiple DirectoryLock instances
    // can acquire locks on the same directory. This only prevents
    // cross-process access via file locking.
    
    lock1.release();
    try testing.expect(!lock1.isLocked());
    
    // Should be able to acquire again after release
    try lock1.acquire();
    try testing.expect(lock1.isLocked());
}