const std = @import("std");

const common = @import("common.zig");
const Change = @import("change.zig").Change;
const SearchResults = common.SearchResults;
const SearchResult = common.SearchResult;
const Scheduler = @import("utils/Scheduler.zig");
const Deadline = @import("utils/Deadline.zig");

const Index = @import("Index.zig");

fn generateRandomHashes(buf: []u32, seed: u64) []u32 {
    var prng = std.rand.DefaultPrng.init(seed);
    const rand = prng.random();
    for (buf) |*h| {
        h.* = std.rand.int(rand, u32);
    }
    return buf;
}

test "index does not exist" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var scheduler = Scheduler.init(std.testing.allocator);
    defer scheduler.deinit();

    var index = try Index.init(std.testing.allocator, &scheduler, tmp_dir.dir, "idx", .{});
    defer index.deinit();

    const result = index.open(false);
    try std.testing.expectError(error.IndexNotFound, result);
}

test "index create, update and search" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var scheduler = Scheduler.init(std.testing.allocator);
    defer scheduler.deinit();

    var index = try Index.init(std.testing.allocator, &scheduler, tmp_dir.dir, "idx", .{});
    defer index.deinit();

    try index.open(true);

    var hashes: [100]u32 = undefined;

    try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = generateRandomHashes(&hashes, 1),
    } }});

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
        var index = try Index.init(std.testing.allocator, &scheduler, tmp_dir.dir, "idx", .{});
        defer index.deinit();

        try index.open(true);

        try index.update(&[_]Change{.{ .insert = .{
            .id = 1,
            .hashes = generateRandomHashes(&hashes, 1),
        } }});
    }

    {
        var index = try Index.init(std.testing.allocator, &scheduler, tmp_dir.dir, "idx", .{});
        defer index.deinit();

        try index.open(false);
        try index.waitForReady(10000);

        var collector = SearchResults.init(std.testing.allocator, .{});
        defer collector.deinit();

        try index.search(generateRandomHashes(&hashes, 1), &collector, .{});

        try std.testing.expectEqualSlices(SearchResult, &.{.{ .id = 1, .score = hashes.len }}, collector.getResults());
    }
}

test "index many updates" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var scheduler = Scheduler.init(std.testing.allocator);
    defer scheduler.deinit();

    try scheduler.start(2);

    var hashes: [100]u32 = undefined;

    {
        var index = try Index.init(std.testing.allocator, &scheduler, tmp_dir.dir, "idx", .{});
        defer index.deinit();

        try index.open(true);

        for (0..100) |i| {
            try index.update(&[_]Change{.{ .insert = .{
                .id = @as(u32, @intCast(i % 20)) + 1,
                .hashes = generateRandomHashes(&hashes, i),
            } }});
        }
    }

    var index = try Index.init(std.testing.allocator, &scheduler, tmp_dir.dir, "idx", .{});
    defer index.deinit();

    try index.open(false);
    try index.waitForReady(100000);

    {
        var collector = SearchResults.init(std.testing.allocator, .{});
        defer collector.deinit();

        try index.search(generateRandomHashes(&hashes, 0), &collector, .{});

        try std.testing.expectEqualSlices(SearchResult, &.{}, collector.getResults());
    }

    {
        var collector = SearchResults.init(std.testing.allocator, .{});
        defer collector.deinit();

        try index.search(generateRandomHashes(&hashes, 80), &collector, .{});

        try std.testing.expectEqualSlices(SearchResult, &.{.{ .id = 1, .score = hashes.len }}, collector.getResults());
    }
}

test "index, multiple fingerprints with the same hashes" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var scheduler = Scheduler.init(std.testing.allocator);
    defer scheduler.deinit();

    var index = try Index.init(std.testing.allocator, &scheduler, tmp_dir.dir, "idx", .{});
    defer index.deinit();

    try index.open(true);

    var hashes: [100]u32 = undefined;

    try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = generateRandomHashes(&hashes, 1),
    } }});

    try index.update(&[_]Change{.{ .insert = .{
        .id = 2,
        .hashes = generateRandomHashes(&hashes, 1),
    } }});

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
