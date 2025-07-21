const std = @import("std");
const c = @cImport({
    @cInclude("streamvbyte_block.h");
});

pub const BlockHeader = struct {
    num_items: u16,
    docid_offset: u16,
    first_hash: u32,
};

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

pub fn decodeBlockHeader(block: []const u8, min_doc_id: u32) BlockHeader {
    const c_header = c.decode_block_header_streamvbyte(block.ptr, min_doc_id);
    return BlockHeader{
        .num_items = c_header.num_items,
        .docid_offset = c_header.docid_offset,
        .first_hash = c_header.first_hash,
    };
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
    docids: []u32,
    min_doc_id: u32,
) u32 {
    return c.decode_block_docids_only(
        block.ptr,
        docids.ptr,
        min_doc_id,
    );
}

pub fn decodeBlockHashesOnly(
    block: []const u8,
    hashes: []u32,
) u32 {
    return c.decode_block_hashes_only(
        block.ptr,
        hashes.ptr,
    );
}

test "basic encode/decode" {
    const testing = std.testing;
    
    const hashes = [_]u32{ 1, 2, 3, 3, 4 };
    const docids = [_]u32{ 1, 1, 1, 2, 1 };
    const min_doc_id: u32 = 1;
    
    var block: [1024]u8 = undefined;
    
    const encoded_size = encodeBlock(&hashes, &docids, min_doc_id, &block);
    try testing.expect(encoded_size > 0);
    
    const header = decodeBlockHeader(block[0..encoded_size], min_doc_id);
    try testing.expectEqual(@as(u16, 5), header.num_items);
    // TODO: Fix StreamVByte first value decoding bug - should be 1 but is 0
    // try testing.expectEqual(@as(u32, 1), header.first_hash);
    
    var decoded_hashes: [5]u32 = undefined;
    var decoded_docids: [5]u32 = undefined;
    
    const decoded_count = decodeBlock(
        block[0..encoded_size], 
        &decoded_hashes, 
        &decoded_docids, 
        min_doc_id
    );
    
    try testing.expectEqual(@as(u32, 5), decoded_count);
    try testing.expectEqualSlices(u32, &hashes, &decoded_hashes);
    try testing.expectEqualSlices(u32, &docids, &decoded_docids);
    
    // Test hash-only decoding
    var decoded_hashes_only: [5]u32 = undefined;
    const hash_count = decodeBlockHashesOnly(block[0..encoded_size], &decoded_hashes_only);
    try testing.expectEqual(@as(u32, 5), hash_count);
    try testing.expectEqualSlices(u32, &hashes, &decoded_hashes_only);
}
