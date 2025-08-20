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

/// Downloads and extracts tar file directly from HTTP URL using piped curl | tar
fn downloadAndExtract(allocator: std.mem.Allocator, url: []const u8, target_dir: std.fs.Dir) !void {
    // Get absolute path for tar extraction
    const target_path = try target_dir.realpathAlloc(allocator, ".");
    defer allocator.free(target_path);

    // Create curl command
    const curl_args = [_][]const u8{
        "curl",
        "-L",          // Follow redirects
        "-f",          // Fail on HTTP errors
        "-s",          // Silent mode
        "-S",          // Show errors even in silent mode
        url,
    };

    // Create tar command
    const tar_args = [_][]const u8{
        "tar",
        "-xf",         // Extract from stdin
        "-",           // Read from stdin
        "-C",          // Change to directory
        target_path,   // Target directory
    };

    log.info("downloading and extracting {s} to {s}", .{ url, target_path });

    // Start curl process
    var curl_child = std.process.Child.init(&curl_args, allocator);
    curl_child.stdout_behavior = .Pipe;
    curl_child.stderr_behavior = .Pipe;

    curl_child.spawn() catch |err| {
        log.err("failed to spawn curl: {}", .{err});
        return error.ProcessSpawnFailed;
    };

    // Start tar process
    var tar_child = std.process.Child.init(&tar_args, allocator);
    tar_child.stdin_behavior = .Pipe;
    tar_child.stdout_behavior = .Ignore;
    tar_child.stderr_behavior = .Pipe;

    tar_child.spawn() catch |err| {
        log.err("failed to spawn tar: {}", .{err});
        // Clean up curl process
        _ = curl_child.kill() catch {};
        return error.ProcessSpawnFailed;
    };

    // Pipe curl stdout to tar stdin
    var pipe_buffer: [8192]u8 = undefined;
    var curl_stdout = curl_child.stdout.?;
    var tar_stdin = tar_child.stdin.?;
    
    // Close tar stdin when we're done to signal EOF
    defer tar_stdin.close();

    // Read from curl and write to tar
    while (true) {
        const bytes_read = curl_stdout.read(&pipe_buffer) catch |err| {
            log.err("failed to read from curl: {}", .{err});
            break;
        };
        
        if (bytes_read == 0) break; // EOF from curl
        
        tar_stdin.writeAll(pipe_buffer[0..bytes_read]) catch |err| {
            log.err("failed to write to tar: {}", .{err});
            break;
        };
    }

    // Close tar stdin to signal EOF
    tar_stdin.close();

    // Read stderr from both processes
    const curl_stderr_content = if (curl_child.stderr) |stderr|
        stderr.readToEndAlloc(allocator, 4096) catch "unknown error"
    else
        "no stderr";
    defer if (curl_child.stderr != null) allocator.free(curl_stderr_content);

    const tar_stderr_content = if (tar_child.stderr) |stderr|
        stderr.readToEndAlloc(allocator, 4096) catch "unknown error"
    else
        "no stderr";
    defer if (tar_child.stderr != null) allocator.free(tar_stderr_content);

    // Wait for both processes to finish
    const curl_term = curl_child.wait() catch |err| {
        log.err("failed to wait for curl: {}", .{err});
        return error.ProcessFailed;
    };

    const tar_term = tar_child.wait() catch |err| {
        log.err("failed to wait for tar: {}", .{err});
        return error.ProcessFailed;
    };

    // Check curl result
    switch (curl_term) {
        .Exited => |code| {
            if (code != 0) {
                log.err("curl failed with exit code {d}: {s}", .{ code, curl_stderr_content });
                return error.NetworkError;
            }
        },
        else => {
            log.err("curl terminated abnormally: {}", .{curl_term});
            return error.ProcessFailed;
        },
    }

    // Check tar result
    switch (tar_term) {
        .Exited => |code| {
            if (code != 0) {
                log.err("tar failed with exit code {d}: {s}", .{ code, tar_stderr_content });
                return error.ExtractionFailed;
            }
        },
        else => {
            log.err("tar terminated abnormally: {}", .{tar_term});
            return error.ProcessFailed;
        },
    }

    // Validate that we have a manifest file
    const manifest_file = target_dir.openFile("manifest", .{}) catch {
        log.err("extracted tar does not contain manifest", .{});
        return error.InvalidTarFormat;
    };
    manifest_file.close();

    log.info("piped download and extraction completed successfully", .{});
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

    switch (source) {
        .local_file => |path| {
            log.info("extracting from local file: {s}", .{path});
            try extractAndValidate(allocator, path, target_dir, source_dir);
        },
        .http_url => |url| {
            log.info("downloading and extracting from URL: {s}", .{url});
            try downloadAndExtract(allocator, url, target_dir);
        },
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