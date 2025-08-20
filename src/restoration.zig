const std = @import("std");
const log = std.log.scoped(.restoration);

const Index = @import("Index.zig");

pub const RestoreSource = union(enum) {
    local_file: []const u8,
    http_url: []const u8,
};

pub const RestoreStatus = enum {
    in_progress,
    completed,
    failed,
};

pub const RestoreError = error{
    FileNotFound,
    InvalidTarFormat,
    NetworkError,
    IndexAlreadyExists,
    ExtractionFailed,
    OutOfMemory,
};

/// Downloads a file from HTTP URL to a temporary location
fn downloadFromHttp(allocator: std.mem.Allocator, url: []const u8, temp_dir: std.fs.Dir) ![]const u8 {
    // For now, return error to implement later with proper HTTP client usage
    _ = allocator;
    _ = url;
    _ = temp_dir;
    log.err("HTTP download not yet implemented", .{});
    return error.NetworkError;
}

/// Extracts tar file to target directory and validates its contents
fn extractAndValidate(_: std.mem.Allocator, tar_path: []const u8, target_dir: std.fs.Dir, source_dir: std.fs.Dir) !void {
    // Open tar file
    var tar_file = source_dir.openFile(tar_path, .{}) catch |err| switch (err) {
        error.FileNotFound => return error.FileNotFound,
        else => return error.InvalidTarFormat,
    };
    defer tar_file.close();

    // Extract tar to target directory
    try std.tar.pipeToFileSystem(target_dir, tar_file.reader(), .{});

    // Validate that we have a manifest file
    const manifest_file = target_dir.openFile("manifest", .{}) catch {
        log.err("extracted tar does not contain manifest", .{});
        return error.InvalidTarFormat;
    };
    manifest_file.close();

    log.info("tar extraction and validation completed successfully", .{});
}

/// Main restoration function that handles the entire restoration process
pub fn restoreFromTar(
    allocator: std.mem.Allocator,
    source: RestoreSource,
    target_dir: std.fs.Dir,
    source_dir: std.fs.Dir,
) !void {
    log.info("starting restoration from tar", .{});

    var tar_path: []const u8 = undefined;
    var should_cleanup = false;
    defer if (should_cleanup) allocator.free(tar_path);

    // Get tar file path (download if needed)
    switch (source) {
        .local_file => |path| {
            tar_path = path;
        },
        .http_url => |url| {
            log.info("downloading from URL: {s}", .{url});
            tar_path = try downloadFromHttp(allocator, url, source_dir);
            should_cleanup = true;
        },
    }

    // Extract and validate
    try extractAndValidate(allocator, tar_path, target_dir, source_dir);

    // Cleanup downloaded file if needed
    if (should_cleanup) {
        source_dir.deleteFile(tar_path) catch |err| {
            log.warn("failed to cleanup temp file {s}: {}", .{ tar_path, err });
        };
    }

    log.info("restoration completed successfully", .{});
}

test "restoration: local file" {
    const Scheduler = @import("utils/Scheduler.zig");
    const Change = @import("change.zig").Change;
    const generateRandomHashes = @import("index_tests.zig").generateRandomHashes;
    const snapshot = @import("snapshot.zig");

    var scheduler = Scheduler.init(std.testing.allocator);
    defer scheduler.deinit();

    try scheduler.start(4);
    defer scheduler.stop();

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    // Create original index and add some data
    var index = try Index.init(std.testing.allocator, &scheduler, tmp_dir.dir, "original", .{
        .min_segment_size = 1,
    });
    defer index.deinit();

    try index.open(true);
    try index.waitForReady(1000);

    var hashes: [100]u32 = undefined;
    _ = try index.update(&[_]Change{.{
        .insert = .{
            .id = 42,
            .hashes = generateRandomHashes(&hashes, 1),
        },
    }}, null, null);

    // Wait for checkpoint
    var retries: usize = 0;
    while (true) {
        var index_reader = try index.acquireReader();
        defer index.releaseReader(&index_reader);

        if (index_reader.file_segments.value.count() > 0) {
            break;
        }

        retries += 1;
        try std.testing.expect(retries < 100);
        std.Thread.sleep(std.time.ns_per_ms * 10);
    }

    // Export snapshot to file
    var snapshot_file = try tmp_dir.dir.createFile("snapshot.tar", .{});
    defer snapshot_file.close();

    try snapshot.buildSnapshot(snapshot_file.writer().any(), &index, std.testing.allocator);

    // Test restoration
    var restore_dir = try tmp_dir.dir.makeOpenPath("restored", .{});
    defer restore_dir.close();

    try restoreFromTar(
        std.testing.allocator,
        .{ .local_file = "snapshot.tar" },
        restore_dir,
        tmp_dir.dir,
    );

    // Verify restored data by opening new index
    var restored_index = try Index.init(std.testing.allocator, &scheduler, tmp_dir.dir, "restored", .{});
    defer restored_index.deinit();

    try restored_index.open(true);
    try restored_index.waitForReady(1000);

    var restored_reader = try restored_index.acquireReader();
    defer restored_index.releaseReader(&restored_reader);

    const doc_info = try restored_reader.getDocInfo(42);
    try std.testing.expect(doc_info != null);
}