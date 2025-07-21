const std = @import("std");
const c = @cImport({
    @cInclude("streamvbyte_block.h");
});

const max_block_size = 4096;

pub fn svb_encode_data(in: u32, out_data: *[*]u8) u2 {
    if (in < (1 << 8)) {
        std.mem.writeInt(u8, *out_data, in, .little);
        out_data += 1;
        return 0;
    } else if (in < (1 << 16)) {
        std.mem.writeInt(u16, *out_data, in, .little);
        out_data += 2;
        return 1;
    } else if (in < (1 << 24)) {
        std.mem.writeInt(u24, *out_data, in, .little);
        out_data += 3;
        return 2;
    } else {
        std.mem.writeInt(u32, *out_data, in, .little);
        out_data += 4;
        return 3;
    }
}

pub fn svb_encode(in: [4]u32, out_key: []u8, out_data: []u8, count: usize) ![]u8 {
    if (count == 0) {
        return out_data[0..];
    }

    const shift: u8 = 0;
    const key: u8 = 0;

    var out_data_ptr: [*]u8 = 0;

    for (0..count) |c| {
        const code = svb_encode_data(in[c], out_data_ptr);
        key |= (code << shift);
        out_data_ptr += code;
        shift += 2;
    }
}

pub const BlockEncoder = struct {
    data: std.BoundedArray(u8, max_block_size + 16),
    min_doc_id: u32 = 0,

    const header_size = 8;

    pub fn init(self: *BlockEncoder, min_doc_id: u32) void {
        self.min_doc_id = min_doc_id;
    }

    pub fn feed(self: *BlockEncoder, hashes: [4]u32, docids: [4]u32) !usize {}
};

pub const BlockHeader = c.block_header_t;

pub fn maxCompressedSize(count: u32) usize {
    return c.streamvbyte_max_compressed_size(count);
}

pub fn encodeBlock(
    hashes: []const u32,
    docids: []const u32,
    min_doc_id: u32,
    block: []u8,
) usize {
    std.debug.assert(hashes.len == docids.len);
    return c.encode_block_streamvbyte(
        hashes.ptr,
        docids.ptr,
        @intCast(hashes.len),
        min_doc_id,
        block.ptr,
        block.len,
    );
}

pub fn decodeBlockHeader(block: []const u8) BlockHeader {
    var c_header: c.block_header_t = undefined;
    c.decode_block_header_streamvbyte(block.ptr, &c_header);
    return c_header;
}

pub fn decodeBlock(
    block: []const u8,
    hashes: []u32,
    docids: []u32,
    min_doc_id: u32,
) u32 {
    std.debug.assert(hashes.len == docids.len);
    return c.decode_block_streamvbyte(
        block.ptr,
        hashes.ptr,
        docids.ptr,
        min_doc_id,
    );
}

pub fn decodeBlockDocidsOnly(
    block: []const u8,
    hashes: []const u32,
    docids: []u32,
    min_doc_id: u32,
) []const u32 {
    std.debug.assert(hashes.len == docids.len);
    const res = c.decode_block_docids_only(
        block.ptr,
        hashes.ptr,
        docids.ptr,
        min_doc_id,
    );
    return docids[0..res];
}

pub fn decodeBlockHashesOnly(
    block: []const u8,
    hashes: []u32,
) []const u32 {
    const res = c.decode_block_hashes_only(
        block.ptr,
        hashes.ptr,
    );
    return hashes[0..res];
}

test "basic encode/decode" {
    const testing = std.testing;

    const hashes = [_]u32{ 1, 2, 3, 3, 4 };
    const docids = [_]u32{ 1, 1, 1, 2, 1 };
    const min_doc_id: u32 = 1;

    var block: [1024]u8 = undefined;

    const encoded_size = encodeBlock(&hashes, &docids, min_doc_id, &block);
    try testing.expect(encoded_size > 0);

    const header = decodeBlockHeader(block[0..encoded_size]);
    try testing.expectEqual(5, header.num_items);
    try testing.expectEqual(1, header.first_hash);

    var decoded_hashes_buf: [10]u32 = undefined;
    var decoded_docids_buf: [10]u32 = undefined;

    // Test hash-only decoding
    const decoded_hashes = decodeBlockHashesOnly(block[0..encoded_size], &decoded_hashes_buf);
    try testing.expectEqual(5, decoded_hashes.len);
    try testing.expectEqualSlices(u32, &hashes, decoded_hashes);

    // Test docid-only decoding (using pre-decoded hashes)
    const decoded_docids = decodeBlockDocidsOnly(block[0..encoded_size], decoded_hashes, &decoded_docids_buf, min_doc_id);
    try testing.expectEqual(5, decoded_docids.len);
    try testing.expectEqualSlices(u32, &docids, decoded_docids);
}
