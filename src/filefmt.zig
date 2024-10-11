const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const math = std.math;

const Item = @import("common.zig").Item;

const minVarint32Size = 1;
const maxVarint32Size = 5;

fn varint32Size(value: u32) usize {
    if (value < (1 << 7)) {
        return 1;
    }
    if (value < (1 << 14)) {
        return 2;
    }
    if (value < (1 << 21)) {
        return 3;
    }
    if (value < (1 << 28)) {
        return 4;
    }
    return maxVarint32Size;
}

test "check varint32Size" {
    try testing.expectEqual(1, varint32Size(1));
    try testing.expectEqual(2, varint32Size(1000));
    try testing.expectEqual(3, varint32Size(100000));
    try testing.expectEqual(4, varint32Size(10000000));
    try testing.expectEqual(5, varint32Size(1000000000));
    try testing.expectEqual(5, varint32Size(math.maxInt(u32)));
}

fn writeVarint32(buf: []u8, value: u32) usize {
    assert(buf.len >= varint32Size(value));
    var v = value;
    var i: usize = 0;
    while (i < maxVarint32Size) : (i += 1) {
        buf[i] = @intCast(v & 0x7F);
        v >>= 7;
        if (v == 0) {
            return i + 1;
        }
        buf[i] |= 0x80;
    }
    unreachable;
}

fn readVarint32(buf: []const u8) struct { value: u32, size: usize } {
    var v: u32 = 0;
    var shift: u5 = 0;
    var i: usize = 0;
    while (i < @min(maxVarint32Size, buf.len)) : (i += 1) {
        const b = buf[i];
        v |= @as(u32, @intCast(b & 0x7F)) << shift;
        shift += 7;
        if (b & 0x80 == 0) {
            return .{ .value = v, .size = i + 1 };
        }
    }
    return .{ .value = v, .size = i };
}

test "check writeVarint32" {
    var buf: [maxVarint32Size]u8 = undefined;

    try std.testing.expectEqual(1, writeVarint32(&buf, 1));
    try std.testing.expectEqualSlices(u8, &[_]u8{0x01}, buf[0..1]);

    try std.testing.expectEqual(2, writeVarint32(&buf, 1000));
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xe8, 0x07 }, buf[0..2]);
}

pub fn minItemsPerBlock(blockSize: usize) usize {
    return (blockSize - 4) / 2;
}

pub fn readBlock(data: []const u8, items: *std.ArrayList(Item)) !void {
    var ptr: usize = 0;

    if (data.len < 2) {
        return error.InvalidBlock;
    }

    const numItems = std.mem.readInt(u16, data[0..2], .little);
    ptr += 2;

    items.clearRetainingCapacity();
    try items.ensureTotalCapacity(numItems);

    var lastHash: u32 = 0;
    var lastDocId: u32 = 0;

    while (ptr + 2 * minVarint32Size < data.len) {
        const diffHash = readVarint32(data[ptr..]);
        ptr += diffHash.size;
        const diffDocId = readVarint32(data[ptr..]);
        ptr += diffDocId.size;

        lastHash += diffHash.value;
        lastDocId = if (diffHash.value > 0) diffDocId.value else lastDocId + diffDocId.value;

        try items.append(.{ .hash = lastHash, .docId = lastDocId });

        if (items.items.len >= numItems) {
            break;
        }
    }

    if (items.items.len < numItems) {
        return error.InvalidBlock;
    }
}

pub fn writeBlock(data: []u8, items: []const Item) !usize {
    assert(data.len >= 2);

    var ptr: usize = 2;
    var numItems: u16 = 0;
    var lastHash: u32 = 0;
    var lastDocId: u32 = 0;

    for (items) |item| {
        assert(item.hash > lastHash or (item.hash == lastHash and item.docId >= lastDocId));

        const diffHash = item.hash - lastHash;
        const diffDocId = if (diffHash > 0) item.docId else item.docId - lastDocId;

        if (ptr + varint32Size(diffHash) + varint32Size(diffDocId) > data.len) {
            break;
        }

        ptr += writeVarint32(data[ptr..], diffHash);
        ptr += writeVarint32(data[ptr..], diffDocId);
        numItems += 1;

        lastHash = item.hash;
        lastDocId = item.docId;
    }

    std.mem.writeInt(u16, data[0..2], numItems, .little);
    @memset(data[ptr..], 0);

    return numItems;
}

test "writeBlock/readBlock" {
    var items = std.ArrayList(Item).init(std.testing.allocator);
    defer items.deinit();

    try items.append(.{ .hash = 1, .docId = 1 });
    try items.append(.{ .hash = 2, .docId = 1 });
    try items.append(.{ .hash = 3, .docId = 1 });
    try items.append(.{ .hash = 3, .docId = 2 });
    try items.append(.{ .hash = 4, .docId = 1 });

    const blockSize = 1024;
    var blockData: [blockSize]u8 = undefined;

    const numItems = try writeBlock(blockData[0..], items.items);
    try testing.expectEqual(items.items.len, numItems);

    items.clearRetainingCapacity();

    try readBlock(blockData[0..], &items);
    try testing.expectEqualSlices(
        Item,
        &[_]Item{
            .{ .hash = 1, .docId = 1 },
            .{ .hash = 2, .docId = 1 },
            .{ .hash = 3, .docId = 1 },
            .{ .hash = 3, .docId = 2 },
            .{ .hash = 4, .docId = 1 },
        },
        items.items,
    );
}
