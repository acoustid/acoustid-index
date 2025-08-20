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
    ProcessSpawnFailed,
    ProcessFailed,
};

/// Downloads a file from HTTP URL to a temporary location using curl subprocess
fn downloadFromHttp(allocator: std.mem.Allocator, url: []const u8, temp_dir: std.fs.Dir) ![]const u8 {
    // Generate a unique filename for the downloaded file
    const filename = try std.fmt.allocPrint(allocator, "download_{d}.tar", .{std.time.timestamp()});
    defer allocator.free(filename);

    // Create the full path for the downloaded file
    const temp_path = try temp_dir.realpathAlloc(allocator, ".");
    defer allocator.free(temp_path);
    const full_path = try std.fs.path.join(allocator, &.{ temp_path, filename });
    defer allocator.free(full_path);

    // Use curl subprocess to download the file
    const curl_args = [_][]const u8{
        "curl",
        "-L",          // Follow redirects
        "-f",          // Fail on HTTP errors
        "-s",          // Silent mode
        "-S",          // Show errors even in silent mode
        "-o", full_path,  // Output file
        url,
    };

    log.info("downloading {s} to {s}", .{ url, full_path });

    var child = std.process.Child.init(&curl_args, allocator);
    child.stdout_behavior = .Ignore;
    child.stderr_behavior = .Pipe;

    child.spawn() catch |err| {
        log.err("failed to spawn curl: {}", .{err});
        return error.ProcessSpawnFailed;
    };

    // Read stderr before waiting
    const stderr_content = if (child.stderr) |stderr|
        stderr.readToEndAlloc(allocator, 4096) catch "unknown error"
    else
        "no stderr";
    defer if (child.stderr != null) allocator.free(stderr_content);

    const term = child.wait() catch |err| {
        log.err("failed to wait for curl: {}", .{err});
        return error.ProcessFailed;
    };

    switch (term) {
        .Exited => |code| {
            if (code != 0) {
                log.err("curl failed with exit code {d}: {s}", .{ code, stderr_content });
                
                // Clean up any partial download
                temp_dir.deleteFile(filename) catch {};
                return error.NetworkError;
            }
        },
        else => {
            log.err("curl terminated abnormally: {}", .{term});
            temp_dir.deleteFile(filename) catch {};
            return error.ProcessFailed;
        },
    }

    // Return the filename (not full path) since caller will use it with temp_dir
    return try allocator.dupe(u8, filename);
}

/// Extracts tar file to target directory using tar subprocess and validates its contents
fn extractAndValidate(allocator: std.mem.Allocator, tar_path: []const u8, target_dir: std.fs.Dir, source_dir: std.fs.Dir) !void {
    // Verify tar file exists
    var tar_file = source_dir.openFile(tar_path, .{}) catch |err| switch (err) {
        error.FileNotFound => return error.FileNotFound,
        else => return error.InvalidTarFormat,
    };
    tar_file.close();

    // Get absolute paths for tar extraction
    const source_path = try source_dir.realpathAlloc(allocator, ".");
    defer allocator.free(source_path);
    const target_path = try target_dir.realpathAlloc(allocator, ".");
    defer allocator.free(target_path);
    const full_tar_path = try std.fs.path.join(allocator, &.{ source_path, tar_path });
    defer allocator.free(full_tar_path);

    // Use tar subprocess to extract the file
    const tar_args = [_][]const u8{
        "tar",
        "-xf",         // Extract from file
        full_tar_path, // Source tar file
        "-C",          // Change to directory
        target_path,   // Target directory
    };

    log.info("extracting {s} to {s}", .{ full_tar_path, target_path });

    var child = std.process.Child.init(&tar_args, allocator);
    child.stdout_behavior = .Ignore;
    child.stderr_behavior = .Pipe;

    child.spawn() catch |err| {
        log.err("failed to spawn tar: {}", .{err});
        return error.ProcessSpawnFailed;
    };

    // Read stderr before waiting
    const stderr_content = if (child.stderr) |stderr|
        stderr.readToEndAlloc(allocator, 4096) catch "unknown error"
    else
        "no stderr";
    defer if (child.stderr != null) allocator.free(stderr_content);

    const term = child.wait() catch |err| {
        log.err("failed to wait for tar: {}", .{err});
        return error.ProcessFailed;
    };

    switch (term) {
        .Exited => |code| {
            if (code != 0) {
                log.err("tar failed with exit code {d}: {s}", .{ code, stderr_content });
                return error.ExtractionFailed;
            }
        },
        else => {
            log.err("tar terminated abnormally: {}", .{term});
            return error.ProcessFailed;
        },
    }

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