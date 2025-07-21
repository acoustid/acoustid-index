#include "streamvbyte_block.h"
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <alloca.h>

#include "streamvbyte_tables.h"

size_t streamvbyte_max_compressed_size(uint32_t count) {
    // Control bytes: 2 bits per value, rounded up to bytes
    size_t control_bytes = (count * 2 + 7) / 8;
    // Data bytes: worst case 4 bytes per value
    size_t data_bytes = count * 4;
    // Add padding for SIMD reads
    return control_bytes + data_bytes + 16;
}

// Generic fallback implementation
size_t streamvbyte_encode_deltas(const uint32_t* in, uint32_t count, uint8_t* out) {
    uint32_t control_bytes = (count + 3) / 4; // 1 control byte per 4 values
    uint8_t* control_ptr = out;
    uint8_t* data_ptr = out + control_bytes;
    
    uint32_t processed = 0;
    
    // Process 4 values at a time
    while (processed + 4 <= count) {
        uint32_t control_byte = 0;
        
        for (int i = 0; i < 4; i++) {
            uint32_t val = in[processed + i];
            uint32_t bytes_needed;
            
            if (val < 0x100) bytes_needed = 1;
            else if (val < 0x10000) bytes_needed = 2;
            else if (val < 0x1000000) bytes_needed = 3;
            else bytes_needed = 4;
            
            control_byte |= ((bytes_needed - 1) << (i * 2));
            
            // Store data bytes
            for (uint32_t b = 0; b < bytes_needed; b++) {
                *data_ptr++ = (val >> (b * 8)) & 0xFF;
            }
        }
        
        *control_ptr++ = control_byte;
        processed += 4;
    }
    
    // Handle remaining values (less than 4)
    if (processed < count) {
        uint32_t remaining = count - processed;
        uint32_t control_byte = 0;
        
        for (uint32_t i = 0; i < remaining; i++) {
            uint32_t val = in[processed + i];
            uint32_t bytes_needed;
            
            if (val < 0x100) bytes_needed = 1;
            else if (val < 0x10000) bytes_needed = 2;
            else if (val < 0x1000000) bytes_needed = 3;
            else bytes_needed = 4;
            
            control_byte |= ((bytes_needed - 1) << (i * 2));
            
            // Store data bytes
            for (uint32_t b = 0; b < bytes_needed; b++) {
                *data_ptr++ = (val >> (b * 8)) & 0xFF;
            }
        }
        
        *control_ptr++ = control_byte;
    }
    
    return data_ptr - out;
}

ssize_t streamvbyte_decode_deltas(const uint8_t* in_ptr, size_t in_len, uint32_t* out, uint32_t count) {
    if (count == 0) {
        return 0;
    }

    // Validate input parameters
    if (in_ptr == NULL || in_len == 0 || out == NULL) {
        return -1;
    }
    
    uint32_t control_bytes = (count + 3) / 4; // 1 control byte per 4 values
    
    // Check if we have enough bytes for control data
    if (in_len < control_bytes) {
        return -1;
    }
    
    const uint8_t* control_ptr = in_ptr;
    const uint8_t* data_ptr = in_ptr + control_bytes;
    const uint8_t* in_end = in_ptr + in_len;
    
    uint32_t processed = 0;

    // Process 4 values at a time
    while (processed + 4 <= count) {
        uint8_t control_byte = *control_ptr++;
        
        for (int i = 0; i < 4; i++) {
            uint32_t bytes_needed = ((control_byte >> (i * 2)) & 3) + 1;
            
            // Check if we have enough bytes left
            if (data_ptr + bytes_needed > in_end) {
                return -1; // Not enough input data
            }
            
            uint32_t val = 0;
            for (uint32_t b = 0; b < bytes_needed; b++) {
                val |= ((uint32_t)*data_ptr++) << (b * 8);
            }
            
            out[processed + i] = val;
        }
        
        processed += 4;
    }
    
    // Handle remaining values
    if (processed < count) {
        // Check if we have another control byte
        if (control_ptr >= in_end) {
            return -1; // Not enough input data
        }
        
        uint8_t control_byte = *control_ptr++;
        uint32_t remaining = count - processed;
        
        for (uint32_t i = 0; i < remaining; i++) {
            uint32_t bytes_needed = ((control_byte >> (i * 2)) & 3) + 1;
            
            // Check if we have enough bytes left
            if (data_ptr + bytes_needed > in_end) {
                return -1; // Not enough input data
            }
            
            uint32_t val = 0;
            for (uint32_t b = 0; b < bytes_needed; b++) {
                val |= ((uint32_t)*data_ptr++) << (b * 8);
            }
            
            out[processed + i] = val;
        }
        
        processed += remaining;
    }
    
    // Return number of bytes consumed
    return data_ptr - in_ptr;
}

size_t encode_block_streamvbyte(
    const uint32_t* hashes, 
    const uint32_t* docids, 
    uint32_t count,
    uint32_t min_doc_id,
    uint8_t* block,
    size_t block_size
) {
    if (count == 0 || block_size < 8) { // Need at least 8 bytes for header now
        return 0;
    }
    
    // Calculate deltas
    uint32_t* hash_deltas = (uint32_t*)alloca(count * sizeof(uint32_t));
    uint32_t* docid_deltas = (uint32_t*)alloca(count * sizeof(uint32_t));
    
    uint32_t last_hash = 0;
    uint32_t last_docid = 0;
    uint32_t first_hash = hashes[0]; // Store first hash for header
    
    for (uint32_t i = 0; i < count; i++) {
        uint32_t hash_delta = hashes[i] - last_hash;
        uint32_t docid_delta = (hash_delta > 0) ? (docids[i] - min_doc_id) : (docids[i] - last_docid);
        
        hash_deltas[i] = hash_delta;
        docid_deltas[i] = docid_delta;
        
        last_hash = hashes[i];
        last_docid = docids[i];
    }
    
    // Encode hashes first
    uint8_t* hash_data = block + 8; // Skip header (now 8 bytes)
    size_t hash_size = streamvbyte_encode_deltas(hash_deltas, count, hash_data);
    
    // Check if we have space for docid data
    size_t header_and_hash_size = 8 + hash_size;
    if (header_and_hash_size >= block_size) {
        return 0; // Not enough space
    }
    
    // Encode docids
    uint8_t* docid_data = block + header_and_hash_size;
    size_t docid_size = streamvbyte_encode_deltas(docid_deltas, count, docid_data);
    
    size_t total_size = header_and_hash_size + docid_size;
    if (total_size > block_size) {
        return 0; // Not enough space
    }
    
    // Write header
    uint16_t docid_offset = (uint16_t)header_and_hash_size;
    memcpy(block, &count, 2);
    memcpy(block + 2, &docid_offset, 2);
    memcpy(block + 4, &first_hash, 4); // Store first hash in header
    
    return total_size;
}

void decode_block_header_streamvbyte(const uint8_t* block, block_header_t *header) {    
    memcpy(&header->num_items, block, 2);
    memcpy(&header->docid_offset, block + 2, 2);    
    memcpy(&header->first_hash, block + 4, 4);    
}

uint32_t decode_block_hashes_only(const uint8_t* block, uint32_t* hashes) {
    uint16_t num_items, docid_offset;
    memcpy(&num_items, block, 2);
    memcpy(&docid_offset, block + 2, 2);
    
    if (num_items == 0) {
        return 0;
    }
    
    // Get first hash from header
    uint32_t first_hash;
    memcpy(&first_hash, block + 4, 4);
    
    // Calculate available bytes for hash deltas (from header to docid offset)
    size_t available_bytes = docid_offset - 8; // 8 bytes for header
    
    // Decode hash deltas (now after the extended header)
    uint32_t* hash_deltas = (uint32_t*)alloca(num_items * sizeof(uint32_t));
    if (streamvbyte_decode_deltas(block + 8, hash_deltas, num_items) == 0) {
        return 0; // Decoding failed
    }
    
    // Convert deltas back to absolute hash values
    uint32_t current_hash = 0;
    for (uint32_t i = 0; i < num_items; i++) {
        current_hash += hash_deltas[i];
        hashes[i] = current_hash;
    }
    
    return num_items;
}

uint32_t decode_block_docids_only(const uint8_t* block, const uint32_t* hashes, uint32_t* docids, uint32_t min_doc_id) {
    uint16_t num_items, docid_offset;
    memcpy(&num_items, block, 2);
    memcpy(&docid_offset, block + 2, 2);
    
    if (num_items == 0) {
        return 0;
    }
    
    // Decode only docid deltas
    uint32_t* docid_deltas = (uint32_t*)alloca(num_items * sizeof(uint32_t));
    if (streamvbyte_decode_deltas(block + docid_offset, docid_deltas, num_items) == 0) {
        return 0; // Decoding failed
    }
    
    // Convert deltas back to absolute docid values
    uint32_t last_docid = 0;
    uint32_t last_hash = 0;
    
    for (uint32_t i = 0; i < num_items; i++) {
        if (hashes[i] != last_hash) {
            last_docid = docid_deltas[i] + min_doc_id;
            last_hash = hashes[i];
        } else {
            last_docid += docid_deltas[i];
        }
        
        docids[i] = last_docid;
    }
    
    return num_items;
}

uint32_t decode_block_streamvbyte(
    const uint8_t* block,
    uint32_t* hashes,
    uint32_t* docids,
    uint32_t min_doc_id
) {
    uint16_t num_items, docid_offset;
    memcpy(&num_items, block, 2);
    memcpy(&docid_offset, block + 2, 2);
    
    if (num_items == 0) {
        return 0;
    }
    
    // Get first hash from header (not used directly but good for debugging)
    uint32_t first_hash;
    memcpy(&first_hash, block + 4, 4);
    
    // Decode deltas
    uint32_t* hash_deltas = (uint32_t*)alloca(num_items * sizeof(uint32_t));
    uint32_t* docid_deltas = (uint32_t*)alloca(num_items * sizeof(uint32_t));
    
    // Calculate available bytes for hash deltas (from header to docid offset)
    size_t hash_available_bytes = docid_offset - 8; // 8 bytes for header
    
    if (streamvbyte_decode_deltas(block + 8, hash_deltas, num_items) == 0) {
        return 0; // Hash decoding failed
    }
    
    if (streamvbyte_decode_deltas(block + docid_offset, docid_deltas, num_items) == 0) {
        return 0; // DocID decoding failed
    }
    
    // Convert deltas back to absolute values
    uint32_t last_hash = 0;
    uint32_t last_docid = 0;
    
    for (uint32_t i = 0; i < num_items; i++) {
        last_hash += hash_deltas[i];
        
        if (hash_deltas[i] > 0) {
            last_docid = docid_deltas[i] + min_doc_id;
        } else {
            last_docid += docid_deltas[i];
        }
        
        if (hashes) hashes[i] = last_hash;
        if (docids) docids[i] = last_docid;
    }
    
    return num_items;
}
