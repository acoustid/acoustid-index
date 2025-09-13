const std = @import("std");

const common = @import("common.zig");
const Change = @import("change.zig").Change;
const SearchResults = common.SearchResults;
const SearchResult = common.SearchResult;
const Scheduler = @import("utils/Scheduler.zig");
const Deadline = @import("utils/Deadline.zig");

const Index = @import("Index.zig");

pub fn generateRandomHashes(buf: []u32, seed: u64) []u32 {
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();
    for (buf) |*h| {
        h.* = rand.int(u32);
    }
    return buf;
}

test "index does not exist" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var scheduler = Scheduler.init(std.testing.allocator);
    defer scheduler.deinit();

    var index = try Index.init(std.testing.allocator, &scheduler, tmp_dir.dir, "idx", "idx", .{});
    defer index.deinit();

    const result = index.open(false);
    try std.testing.expectError(error.IndexNotFound, result);
}

test "index create, update and search" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var scheduler = Scheduler.init(std.testing.allocator);
    defer scheduler.deinit();

    var index = try Index.init(std.testing.allocator, &scheduler, tmp_dir.dir, "idx", "idx", .{});
    defer index.deinit();

    try index.open(true);

    var hashes: [100]u32 = undefined;

    _ = try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = generateRandomHashes(&hashes, 1),
    } }}, null, .{});

    {
        var collector = SearchResults.init(std.testing.allocator, .{});
        defer collector.deinit();

        try index.search(generateRandomHashes(&hashes, 1), &collector, .{});

        try std.testing.expectEqualSlices(SearchResult, &.{.{ .id = 1, .score = hashes.len }}, collector.getResults());
    }

    {
        var collector = SearchResults.init(std.testing.allocator, .{});
        defer collector.deinit();

        try index.search(generateRandomHashes(&hashes, 999), &collector, .{});

        try std.testing.expectEqualSlices(SearchResult, &.{}, collector.getResults());
    }
}

test "index create, update, reopen and search" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var scheduler = Scheduler.init(std.testing.allocator);
    defer scheduler.deinit();

    try scheduler.start(2);

    var hashes: [100]u32 = undefined;

    {
        var index = try Index.init(std.testing.allocator, &scheduler, tmp_dir.dir, "idx", "idx", .{});
        defer index.deinit();

        try index.open(true);

        _ = try index.update(&[_]Change{.{ .insert = .{
            .id = 1,
            .hashes = generateRandomHashes(&hashes, 1),
        } }}, null, .{});
    }

    {
        var index = try Index.init(std.testing.allocator, &scheduler, tmp_dir.dir, "idx", "idx", .{});
        defer index.deinit();

        try index.open(false);

        var collector = SearchResults.init(std.testing.allocator, .{});
        defer collector.deinit();

        try index.search(generateRandomHashes(&hashes, 1), &collector, .{});

        try std.testing.expectEqualSlices(SearchResult, &.{.{ .id = 1, .score = hashes.len }}, collector.getResults());
    }
}

test "index many updates and inserts" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var scheduler = Scheduler.init(std.testing.allocator);
    defer scheduler.deinit();
    try scheduler.start(2);

    var index = try Index.init(std.testing.allocator, &scheduler, tmp_dir.dir, "idx", "idx", .{
        .min_segment_size = 50_000,
        .max_segment_size = 75_000_000,
    });
    defer index.deinit();
    try index.open(true);

    var hashes: [100]u32 = undefined;

    // Test 1: Individual inserts with duplicate IDs (testing updates)
    for (0..100) |i| {
        _ = try index.update(&[_]Change{.{ .insert = .{
            .id = @as(u32, @intCast(i % 20)) + 1,
            .hashes = generateRandomHashes(&hashes, i),
        } }}, null, .{});
    }

    // Test 2: Batch inserts with larger scale
    const batch_size = 100;
    const total_count = 5000;
    const max_hash = 1 << 18; // 2^18

    var batch = std.ArrayList(Change).init(std.testing.allocator);
    defer batch.deinit();

    var i: u32 = 21; // Continue from previous phase
    while (i <= total_count) : (i += 1) {
        // Generate hashes with deterministic seed based on ID
        var prng = std.Random.DefaultPrng.init(i);
        const rand = prng.random();
        for (&hashes) |*h| {
            h.* = rand.int(u32) % max_hash;
        }

        try batch.append(.{ .insert = .{
            .id = i,
            .hashes = try std.testing.allocator.dupe(u32, &hashes),
        } });

        if (batch.items.len == batch_size or i == total_count) {
            _ = try index.update(batch.items, null, .{});

            // Clean up allocated hashes
            for (batch.items) |change| {
                std.testing.allocator.free(change.insert.hashes);
            }
            batch.clearRetainingCapacity();
        }
    }

    // Verification tests
    {
        // Verify no results for non-existent hashes
        var collector = SearchResults.init(std.testing.allocator, .{});
        defer collector.deinit();
        try index.search(generateRandomHashes(&hashes, 0), &collector, .{});
        try std.testing.expectEqualSlices(SearchResult, &.{}, collector.getResults());
    }

    {
        // Verify ID 100 exists with expected score
        var prng = std.Random.DefaultPrng.init(100);
        const rand = prng.random();
        for (&hashes) |*h| {
            h.* = rand.int(u32) % max_hash;
        }

        var collector = SearchResults.init(std.testing.allocator, .{});
        defer collector.deinit();
        try index.search(&hashes, &collector, .{});
        try std.testing.expectEqualSlices(SearchResult, &.{.{ .id = 100, .score = hashes.len }}, collector.getResults());
    }

    // Verify segment management - check for multiple file segments and merging evidence
    {
        var reader = try index.acquireReader();
        defer index.releaseReader(&reader);

        var file_segments: usize = 0;
        var file_merges: u64 = 0;

        // Count file segments and merges
        for (reader.file_segments.value.nodes.items) |node| {
            file_segments += 1;
            file_merges += node.value.info.merges;
        }

        // Verify evidence of file segment merging (checkpointing and merging)
        try std.testing.expect(file_segments >= 1);
        try std.testing.expect(file_merges > 0);
    }
}

test "index, multiple fingerprints with the same hashes" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var scheduler = Scheduler.init(std.testing.allocator);
    defer scheduler.deinit();

    var index = try Index.init(std.testing.allocator, &scheduler, tmp_dir.dir, "idx", "idx", .{});
    defer index.deinit();

    try index.open(true);

    var hashes: [100]u32 = undefined;

    _ = try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = generateRandomHashes(&hashes, 1),
    } }}, null, .{});

    _ = try index.update(&[_]Change{.{ .insert = .{
        .id = 2,
        .hashes = generateRandomHashes(&hashes, 1),
    } }}, null, .{});

    var collector = SearchResults.init(std.testing.allocator, .{});
    defer collector.deinit();

    try index.search(generateRandomHashes(&hashes, 1), &collector, .{});

    try std.testing.expectEqualSlices(SearchResult, &.{
        .{
            .id = 1,
            .score = hashes.len,
        },
        .{
            .id = 2,
            .score = hashes.len,
        },
    }, collector.getResults());
}
