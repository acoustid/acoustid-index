const std = @import("std");
const fs = std.fs;
const io = std.io;

/// TAR header structure for streaming tar files
const TarHeader = extern struct {
    name: [100]u8,
    mode: [8]u8,
    uid: [8]u8,
    gid: [8]u8,
    size: [12]u8,
    mtime: [12]u8,
    checksum: [8]u8,
    type_flag: u8,
    link_name: [100]u8,
    magic: [6]u8,
    version: [2]u8,
    uname: [32]u8,
    gname: [32]u8,
    dev_major: [8]u8,
    dev_minor: [8]u8,
    prefix: [155]u8,
    padding: [12]u8,

    const Self = @This();

    pub fn init() Self {
        var header = Self{
            .name = std.mem.zeroes([100]u8),
            .mode = std.mem.zeroes([8]u8),
            .uid = std.mem.zeroes([8]u8),
            .gid = std.mem.zeroes([8]u8),
            .size = std.mem.zeroes([12]u8),
            .mtime = std.mem.zeroes([12]u8),
            .checksum = std.mem.zeroes([8]u8),
            .type_flag = '0', // Regular file
            .link_name = std.mem.zeroes([100]u8),
            .magic = "ustar\x00".*,
            .version = "00".*,
            .uname = std.mem.zeroes([32]u8),
            .gname = std.mem.zeroes([32]u8),
            .dev_major = std.mem.zeroes([8]u8),
            .dev_minor = std.mem.zeroes([8]u8),
            .prefix = std.mem.zeroes([155]u8),
            .padding = std.mem.zeroes([12]u8),
        };

        // Set default mode (644)
        _ = std.fmt.bufPrint(&header.mode, "{o:0>7}\x00", .{0o644}) catch unreachable;
        // Set default uid/gid (0)
        _ = std.fmt.bufPrint(&header.uid, "{o:0>7}\x00", .{0}) catch unreachable;
        _ = std.fmt.bufPrint(&header.gid, "{o:0>7}\x00", .{0}) catch unreachable;

        return header;
    }

    pub fn setName(self: *Self, name: []const u8) void {
        const copy_len = @min(name.len, self.name.len - 1);
        @memcpy(self.name[0..copy_len], name[0..copy_len]);
        self.name[copy_len] = 0;
    }

    pub fn setSize(self: *Self, size: u64) void {
        _ = std.fmt.bufPrint(&self.size, "{o:0>11}\x00", .{size}) catch unreachable;
    }

    pub fn setMtime(self: *Self, mtime: u64) void {
        _ = std.fmt.bufPrint(&self.mtime, "{o:0>11}\x00", .{mtime}) catch unreachable;
    }

    pub fn updateChecksum(self: *Self) void {
        // Clear checksum field first
        @memset(&self.checksum, ' ');

        // Calculate checksum
        const bytes = std.mem.asBytes(self);
        var sum: u32 = 0;
        for (bytes) |byte| {
            sum += byte;
        }

        // Write checksum with null terminator
        _ = std.fmt.bufPrint(&self.checksum, "{o:0>6}\x00 ", .{sum}) catch unreachable;
    }
};

/// Streaming tar writer that doesn't require keeping files in memory
pub const TarWriter = struct {
    writer: std.io.AnyWriter,

    const Self = @This();

    pub fn init(writer: std.io.AnyWriter) Self {
        return Self{ .writer = writer };
    }

    /// Add a file to the tar stream by reading from the provided file
    pub fn addFile(self: *Self, name: []const u8, file: fs.File) !void {
        const stat = try file.stat();
        
        var header = TarHeader.init();
        header.setName(name);
        header.setSize(stat.size);
        header.setMtime(@intCast(@divTrunc(stat.mtime, std.time.ns_per_s)));
        header.updateChecksum();

        // Write header
        try self.writer.writeAll(std.mem.asBytes(&header));

        // Copy file contents in chunks
        const chunk_size = 8192;
        var buffer: [chunk_size]u8 = undefined;
        var bytes_written: u64 = 0;

        while (bytes_written < stat.size) {
            const bytes_to_read = @min(chunk_size, stat.size - bytes_written);
            const bytes_read = try file.readAll(buffer[0..bytes_to_read]);
            if (bytes_read == 0) break;
            
            try self.writer.writeAll(buffer[0..bytes_read]);
            bytes_written += bytes_read;
        }

        // Pad to 512-byte boundary
        const padding_needed = (512 - (stat.size % 512)) % 512;
        if (padding_needed > 0) {
            const padding = [_]u8{0} ** 512;
            try self.writer.writeAll(padding[0..padding_needed]);
        }
    }

    /// Add file contents from memory
    pub fn addFileFromMemory(self: *Self, name: []const u8, contents: []const u8) !void {
        var header = TarHeader.init();
        header.setName(name);
        header.setSize(contents.len);
        header.setMtime(@intCast(std.time.timestamp()));
        header.updateChecksum();

        // Write header
        try self.writer.writeAll(std.mem.asBytes(&header));

        // Write contents
        try self.writer.writeAll(contents);

        // Pad to 512-byte boundary
        const padding_needed = (512 - (contents.len % 512)) % 512;
        if (padding_needed > 0) {
            const padding = [_]u8{0} ** 512;
            try self.writer.writeAll(padding[0..padding_needed]);
        }
    }

    /// Finalize the tar stream by writing end-of-archive markers
    pub fn finish(self: *Self) !void {
        // Write two 512-byte zero blocks to mark end of archive
        const zero_block = [_]u8{0} ** 512;
        try self.writer.writeAll(&zero_block);
        try self.writer.writeAll(&zero_block);
    }
};

test "TarHeader size" {
    const testing = std.testing;
    try testing.expectEqual(512, @sizeOf(TarHeader));
}

test "TarHeader checksum" {
    var header = TarHeader.init();
    header.setName("test.txt");
    header.setSize(13);
    header.setMtime(1234567890);
    header.updateChecksum();

    // Verify checksum is properly null-terminated
    const checksum_str = std.mem.sliceTo(&header.checksum, 0);
    _ = std.fmt.parseUnsigned(u32, checksum_str, 8) catch unreachable;
}