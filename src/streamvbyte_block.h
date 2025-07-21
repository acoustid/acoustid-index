#ifndef STREAMVBYTE_BLOCK_H
#define STREAMVBYTE_BLOCK_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

// Block format: <num_items:u16> <docid_offset:u16> <hash_data> <docid_data>
// Both hash and docid arrays are delta-encoded before streamvbyte compression

typedef struct {
    uint16_t num_items;
    uint16_t docid_offset;  // Offset to docid data (from start of block)
    uint32_t first_hash;
} block_header_t;

// Maximum size needed for encoded data (worst case)
size_t streamvbyte_max_compressed_size(uint32_t count);

// Encode delta-compressed arrays into block format
// Returns total bytes written to block
size_t encode_block_streamvbyte(
    const uint32_t* hashes, 
    const uint32_t* docids, 
    uint32_t count,
    uint32_t min_doc_id,
    uint8_t* block,
    size_t block_size
);

// Decode complete block (both docids and hashes)
// Returns number of items decoded
uint32_t decode_block_streamvbyte(
    const uint8_t* block,
    uint32_t* hashes,
    uint32_t* docids,
    uint32_t min_doc_id
);

// Decode only docids (for fast docid-only queries)
// Returns number of items decoded
uint32_t decode_block_docids_only(
    const uint8_t* block,
    const uint32_t* hashes,
    uint32_t* docids,
    uint32_t min_doc_id
);

// Decode only hashes (for fast hash-only queries)
// Returns number of items decoded
uint32_t decode_block_hashes_only(
    const uint8_t* block,
    uint32_t* hashes
);

// Get block header without full decode
void decode_block_header_streamvbyte(
    const uint8_t* block,
    block_header_t *header
);

// Internal functions for StreamVByte encoding/decoding
size_t streamvbyte_encode_deltas(const uint32_t* in, uint32_t length, uint8_t* out);
ssize_t streamvbyte_decode_deltas(const uint8_t* in, size_t in_len, uint32_t* out, uint32_t count);

#ifdef __cplusplus
}
#endif

#endif // STREAMVBYTE_BLOCK_H
