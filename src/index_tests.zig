const std = @import("std");

const common = @import("common.zig");
const Change = @import("change.zig").Change;
const SearchResults = common.SearchResults;

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

    var index = try Index.init(std.testing.allocator, tmp_dir.dir, .{});
    defer index.deinit();

    const result = index.open();
    try std.testing.expectError(error.IndexNotFound, result);
}

test "index create, update and search" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var index = try Index.init(std.testing.allocator, tmp_dir.dir, .{ .create = true });
    defer index.deinit();

    try index.open();

    var hashes: [100]u32 = undefined;

    try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = generateRandomHashes(&hashes, 1),
    } }});

    {
        var results = try index.search(generateRandomHashes(&hashes, 1), std.testing.allocator, .{});
        defer results.deinit();

        try std.testing.expectEqual(1, results.count());

        const result = results.get(1);
        try std.testing.expect(result != null);
        try std.testing.expectEqual(1, result.?.id);
        try std.testing.expectEqual(hashes.len, result.?.score);
    }

    {
        var results = try index.search(generateRandomHashes(&hashes, 999), std.testing.allocator, .{});
        defer results.deinit();

        try std.testing.expectEqual(0, results.count());
    }
}

test "index create, update, reopen and search" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var hashes: [100]u32 = undefined;

    {
        var index = try Index.init(std.testing.allocator, tmp_dir.dir, .{ .create = true });
        defer index.deinit();

        try index.open();

        try index.update(&[_]Change{.{ .insert = .{
            .id = 1,
            .hashes = generateRandomHashes(&hashes, 1),
        } }});
    }

    {
        var index = try Index.init(std.testing.allocator, tmp_dir.dir, .{ .create = false });
        defer index.deinit();

        try index.open();

        var results = try index.search(generateRandomHashes(&hashes, 1), std.testing.allocator, .{});
        defer results.deinit();

        try std.testing.expectEqual(1, results.count());

        const result = results.get(1);
        try std.testing.expect(result != null);
        try std.testing.expectEqual(1, result.?.id);
        try std.testing.expectEqual(hashes.len, result.?.score);
    }
}

// test "index many updates" {
//     var tmp_dir = std.testing.tmpDir(.{});
//     defer tmp_dir.cleanup();

//     var hashes: [100]u32 = undefined;

//     {
//         var index = try Index.init(std.testing.allocator, tmp_dir.dir, .{ .create = true });
//         defer index.deinit();

//         try index.open();

//         for (0..100) |i| {
//             try index.update(&[_]Change{.{ .insert = .{
//                 .id = @as(u32, @intCast(i % 20)) + 1,
//                 .hashes = generateRandomHashes(&hashes, i),
//             } }});
//         }
//     }

//     var index = try Index.init(std.testing.allocator, tmp_dir.dir, .{ .create = false });
//     defer index.deinit();

//     try index.open();

//     {
//         var results = try index.search(generateRandomHashes(&hashes, 0), std.testing.allocator, .{});
//         defer results.deinit();

//         const result = results.get(1);
//         try std.testing.expect(result == null or result.?.score == 0);
//     }

//     {
//         var results = try index.search(generateRandomHashes(&hashes, 80), std.testing.allocator, .{});
//         defer results.deinit();

//         const result = results.get(1);
//         try std.testing.expect(result != null);
//         try std.testing.expectEqual(1, result.?.id);
//         try std.testing.expectEqual(hashes.len, result.?.score);
//     }
// }

test "index, multiple fingerprints with the same hashes" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var index = try Index.init(std.testing.allocator, tmp_dir.dir, .{ .create = true });
    defer index.deinit();

    try index.open();

    var hashes: [100]u32 = undefined;

    try index.update(&[_]Change{.{ .insert = .{
        .id = 1,
        .hashes = generateRandomHashes(&hashes, 1),
    } }});

    try index.update(&[_]Change{.{ .insert = .{
        .id = 2,
        .hashes = generateRandomHashes(&hashes, 1),
    } }});

    var results = try index.search(generateRandomHashes(&hashes, 1), std.testing.allocator, .{});
    defer results.deinit();

    try std.testing.expectEqual(2, results.count());

    if (results.get(1)) |result| {
        try std.testing.expectEqual(1, result.id);
        try std.testing.expectEqual(hashes.len, result.score);
    } else {
        try std.testing.expect(false);
    }

    if (results.get(2)) |result| {
        try std.testing.expectEqual(2, result.id);
        try std.testing.expectEqual(hashes.len, result.score);
    } else {
        try std.testing.expect(false);
    }
}
