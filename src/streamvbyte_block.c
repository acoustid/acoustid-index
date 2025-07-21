#include "streamvbyte_block.h"
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <alloca.h>

#if defined(__x86_64__) || defined(_M_X64)
#define STREAMVBYTE_X64 1
#include <immintrin.h>
#elif defined(__aarch64__) || defined(_M_ARM64)
#define STREAMVBYTE_ARM64 1
#include <arm_neon.h>
#endif

#include "streamvbyte_tables.h"

// Forward declarations
static size_t streamvbyte_encode_generic(const uint32_t* in, uint32_t count, uint8_t* out);
static size_t streamvbyte_decode_generic(const uint8_t* in, uint32_t* out, uint32_t count);

size_t streamvbyte_max_compressed_size(uint32_t count) {
    // Control bytes: 2 bits per value, rounded up to bytes
    size_t control_bytes = (count * 2 + 7) / 8;
    // Data bytes: worst case 4 bytes per value
    size_t data_bytes = count * 4;
    // Add padding for SIMD reads
    return control_bytes + data_bytes + 16;
}

#ifdef STREAMVBYTE_X64
// SSE4.1 optimized encode using GCC function multiversioning
__attribute__((target("sse4.1")))
size_t streamvbyte_encode_deltas(const uint32_t* in, uint32_t count, uint8_t* out) {
    uint32_t control_bytes = (count + 3) / 4; // 1 control byte per 4 values
    uint8_t* control_ptr = out;
    uint8_t* data_ptr = out + control_bytes;
    
    uint32_t processed = 0;
    
    // Process 4 values at a time
    while (processed + 4 <= count) {
        __m128i values = _mm_loadu_si128((const __m128i*)(in + processed));
        
        // Determine bytes needed for each value
        uint32_t control_byte = 0;
        uint32_t val[4];
        _mm_storeu_si128((__m128i*)val, values);
        
        for (int i = 0; i < 4; i++) {
            uint32_t bytes_needed;
            if (val[i] < 0x100) bytes_needed = 1;
            else if (val[i] < 0x10000) bytes_needed = 2;
            else if (val[i] < 0x1000000) bytes_needed = 3;
            else bytes_needed = 4;
            
            control_byte |= ((bytes_needed - 1) << (i * 2));
        }
        
        // Store control byte
        *control_ptr++ = control_byte;
        
        // Use shuffle table to pack the data
        __m128i shuffled = _mm_shuffle_epi8(values, _mm_loadu_si128((const __m128i*)shuffle_table[control_byte]));
        _mm_storeu_si128((__m128i*)data_ptr, shuffled);
        data_ptr += length_table[control_byte];
        
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

// SSE4.1 optimized decode using GCC function multiversioning
__attribute__((target("sse4.1")))
size_t streamvbyte_decode_deltas(const uint8_t* in, uint32_t* out, uint32_t count) {
    uint32_t control_bytes = (count + 3) / 4; // 1 control byte per 4 values
    const uint8_t* control_ptr = in;
    const uint8_t* data_ptr = in + control_bytes;
    
    uint32_t processed = 0;
    
    // Process 4 values at a time
    while (processed + 4 <= count) {
        uint8_t control_byte = *control_ptr++;
        
        // Load packed data using length table
        __m128i packed_data = _mm_loadu_si128((const __m128i*)data_ptr);
        
        // Use inverse shuffle to unpack (for now, fall back to scalar)
        uint32_t val[4] = {0, 0, 0, 0};
        const uint8_t* read_ptr = data_ptr;
        
        for (int i = 0; i < 4; i++) {
            uint32_t bytes_needed = ((control_byte >> (i * 2)) & 3) + 1;
            memcpy(&val[i], read_ptr, bytes_needed);
            read_ptr += bytes_needed;
        }
        
        // Store the 4 values
        _mm_storeu_si128((__m128i*)(out + processed), _mm_loadu_si128((const __m128i*)val));
        
        data_ptr += length_table[control_byte];
        processed += 4;
    }
    
    // Handle remaining values
    if (processed < count) {
        uint8_t control_byte = *control_ptr++;
        uint32_t remaining = count - processed;
        
        for (uint32_t i = 0; i < remaining; i++) {
            uint32_t bytes_needed = ((control_byte >> (i * 2)) & 3) + 1;
            uint32_t val = 0;
            memcpy(&val, data_ptr, bytes_needed);
            data_ptr += bytes_needed;
            out[processed + i] = val;
        }
    }
    
    return data_ptr - in;
}
#endif

#ifdef STREAMVBYTE_ARM64
// ARM64 NEON optimized encode
static size_t streamvbyte_encode_neon(const uint32_t* in, uint32_t count, uint8_t* out) {
    uint32_t control_bytes = (count + 3) / 4; // 1 control byte per 4 values
    uint8_t* control_ptr = out;
    uint8_t* data_ptr = out + control_bytes;
    
    uint32_t processed = 0;
    
    // Process 4 values at a time using NEON
    while (processed + 4 <= count) {
        uint32x4_t values = vld1q_u32(in + processed);
        
        // Determine bytes needed for each value
        uint32_t control_byte = 0;
        uint32_t val[4];
        vst1q_u32(val, values);
        
        for (int i = 0; i < 4; i++) {
            uint32_t bytes_needed;
            if (val[i] < 0x100) bytes_needed = 1;
            else if (val[i] < 0x10000) bytes_needed = 2;
            else if (val[i] < 0x1000000) bytes_needed = 3;
            else bytes_needed = 4;
            
            control_byte |= ((bytes_needed - 1) << (i * 2));
        }
        
        // Store control byte
        *control_ptr++ = control_byte;
        
        // Pack data using NEON (simplified - could be optimized further with table lookup)
        for (int i = 0; i < 4; i++) {
            uint32_t bytes_needed = ((control_byte >> (i * 2)) & 3) + 1;
            for (uint32_t b = 0; b < bytes_needed; b++) {
                *data_ptr++ = (val[i] >> (b * 8)) & 0xFF;
            }
        }
        
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

// ARM64 NEON optimized decode
static size_t streamvbyte_decode_neon(const uint8_t* in, uint32_t* out, uint32_t count) {
    uint32_t control_bytes = (count + 3) / 4; // 1 control byte per 4 values
    const uint8_t* control_ptr = in;
    const uint8_t* data_ptr = in + control_bytes;
    
    uint32_t processed = 0;
    
    // Process 4 values at a time using NEON
    while (processed + 4 <= count) {
        uint8_t control_byte = *control_ptr++;
        
        // Decode 4 values
        uint32_t val[4] = {0, 0, 0, 0};
        const uint8_t* read_ptr = data_ptr;
        
        for (int i = 0; i < 4; i++) {
            uint32_t bytes_needed = ((control_byte >> (i * 2)) & 3) + 1;
            for (uint32_t b = 0; b < bytes_needed; b++) {
                val[i] |= ((uint32_t)*read_ptr++) << (b * 8);
            }
        }
        
        // Store the 4 values using NEON
        uint32x4_t result = vld1q_u32(val);
        vst1q_u32(out + processed, result);
        
        data_ptr += length_table[control_byte];
        processed += 4;
    }
    
    // Handle remaining values
    if (processed < count) {
        uint8_t control_byte = *control_ptr++;
        uint32_t remaining = count - processed;
        
        for (uint32_t i = 0; i < remaining; i++) {
            uint32_t bytes_needed = ((control_byte >> (i * 2)) & 3) + 1;
            uint32_t val = 0;
            for (uint32_t b = 0; b < bytes_needed; b++) {
                val |= ((uint32_t)*data_ptr++) << (b * 8);
            }
            out[processed + i] = val;
        }
    }
    
    return data_ptr - in;
}

// ARM64 always has NEON - no runtime detection needed
size_t streamvbyte_encode_deltas(const uint32_t* in, uint32_t count, uint8_t* out) {
    return streamvbyte_encode_neon(in, count, out);
}

size_t streamvbyte_decode_deltas(const uint8_t* in, uint32_t* out, uint32_t count) {
    return streamvbyte_decode_neon(in, out, count);
}
#endif

#ifdef STREAMVBYTE_X64
// x86_64 versions using GCC function multiversioning
__attribute__((target("default")))
size_t streamvbyte_encode_deltas(const uint32_t* in, uint32_t count, uint8_t* out) {
    return streamvbyte_encode_generic(in, count, out);
}

__attribute__((target("default")))
size_t streamvbyte_decode_deltas(const uint8_t* in, uint32_t* out, uint32_t count) {
    return streamvbyte_decode_generic(in, out, count);
}
#endif

#if !defined(STREAMVBYTE_ARM64) && !defined(STREAMVBYTE_X64)
// Non-SIMD platforms: use generic implementation directly
size_t streamvbyte_encode_deltas(const uint32_t* in, uint32_t count, uint8_t* out) {
    return streamvbyte_encode_generic(in, count, out);
}

size_t streamvbyte_decode_deltas(const uint8_t* in, uint32_t* out, uint32_t count) {
    return streamvbyte_decode_generic(in, out, count);
}
#endif

// Generic fallback implementation
static size_t streamvbyte_encode_generic(const uint32_t* in, uint32_t count, uint8_t* out) {
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

static size_t streamvbyte_decode_generic(const uint8_t* in, uint32_t* out, uint32_t count) {
    uint32_t control_bytes = (count + 3) / 4; // 1 control byte per 4 values
    const uint8_t* control_ptr = in;
    const uint8_t* data_ptr = in + control_bytes;
    
    uint32_t processed = 0;
    
    // Process 4 values at a time
    while (processed + 4 <= count) {
        uint8_t control_byte = *control_ptr++;
        
        for (int i = 0; i < 4; i++) {
            uint32_t bytes_needed = ((control_byte >> (i * 2)) & 3) + 1;
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
        uint8_t control_byte = *control_ptr++;
        uint32_t remaining = count - processed;
        
        for (uint32_t i = 0; i < remaining; i++) {
            uint32_t bytes_needed = ((control_byte >> (i * 2)) & 3) + 1;
            uint32_t val = 0;
            
            for (uint32_t b = 0; b < bytes_needed; b++) {
                val |= ((uint32_t)*data_ptr++) << (b * 8);
            }
            
            out[processed + i] = val;
        }
    }
    
    return data_ptr - in;
}



size_t encode_block_streamvbyte(
    const uint32_t* hashes, 
    const uint32_t* docids, 
    uint32_t count,
    uint32_t min_doc_id,
    uint8_t* block,
    size_t block_size
) {
    if (count == 0 || block_size < 4) {
        return 0;
    }
    
    // Calculate deltas
    uint32_t* hash_deltas = (uint32_t*)alloca(count * sizeof(uint32_t));
    uint32_t* docid_deltas = (uint32_t*)alloca(count * sizeof(uint32_t));
    
    uint32_t last_hash = 0;
    uint32_t last_docid = 0;
    
    for (uint32_t i = 0; i < count; i++) {
        uint32_t hash_delta = hashes[i] - last_hash;
        uint32_t docid_delta = (hash_delta > 0) ? (docids[i] - min_doc_id) : (docids[i] - last_docid);
        
        hash_deltas[i] = hash_delta;
        docid_deltas[i] = docid_delta;
        
        last_hash = hashes[i];
        last_docid = docids[i];
    }
    
    // Encode hashes first
    uint8_t* hash_data = block + 4; // Skip header
    size_t hash_size = streamvbyte_encode_deltas(hash_deltas, count, hash_data);
    
    // Check if we have space for docid data
    size_t header_and_hash_size = 4 + hash_size;
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
    
    return total_size;
}

block_header_t decode_block_header_streamvbyte(const uint8_t* block, uint32_t min_doc_id __attribute__((unused))) {
    block_header_t header = {0};
    
    memcpy(&header.num_items, block, 2);
    memcpy(&header.docid_offset, block + 2, 2);
    
    if (header.num_items > 0) {
        // Decode first hash delta
        uint32_t first_hash_delta;
        streamvbyte_decode_deltas(block + 4, &first_hash_delta, 1);
        
        // The first hash delta is the hash itself (since last_hash starts at 0)
        header.first_hash = first_hash_delta;
    }
    
    return header;
}

uint32_t decode_block_hashes_only(const uint8_t* block, uint32_t* hashes) {
    uint16_t num_items, docid_offset;
    memcpy(&num_items, block, 2);
    memcpy(&docid_offset, block + 2, 2);
    
    if (num_items == 0) {
        return 0;
    }
    
    // Decode hash deltas (now first in the block)
    uint32_t* hash_deltas = (uint32_t*)alloca(num_items * sizeof(uint32_t));
    streamvbyte_decode_deltas(block + 4, hash_deltas, num_items);
    
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
    streamvbyte_decode_deltas(block + docid_offset, docid_deltas, num_items);
    
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
    
    // Decode deltas
    uint32_t* hash_deltas = (uint32_t*)alloca(num_items * sizeof(uint32_t));
    uint32_t* docid_deltas = (uint32_t*)alloca(num_items * sizeof(uint32_t));
    
    streamvbyte_decode_deltas(block + 4, hash_deltas, num_items);
    streamvbyte_decode_deltas(block + docid_offset, docid_deltas, num_items);
    
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
