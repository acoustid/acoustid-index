#include "streamvbyte_decode.h"
#include "streamvbyte_tables.h"
#include <string.h>
#include <immintrin.h>
#include <cpuid.h>

// CPU feature detection
static int has_sse41(void) {
    unsigned int eax, ebx, ecx, edx;
    if (__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
        return (ecx & bit_SSE4_1) != 0;
    }
    return 0;
}

// Generic scalar implementations
static size_t svb_decode_quad_0124_scalar(uint8_t control, const uint8_t* in_data, uint32_t* out) {
    const uint8_t* in_ptr = in_data;
    
    for (int i = 0; i < 4; i++) {
        uint8_t code = (control >> (2 * i)) & 0x3;
        
        if (code == 0) {
            // 0 bytes - value is zero
            out[i] = 0;
        } else if (code == 1) {
            // 1 byte
            out[i] = *in_ptr;
            in_ptr += 1;
        } else if (code == 2) {
            // 2 bytes
            uint32_t val = 0;
            memcpy(&val, in_ptr, 2);
            out[i] = val;
            in_ptr += 2;
        } else { // code == 3
            // 4 bytes
            uint32_t val = 0;
            memcpy(&val, in_ptr, 4);
            out[i] = val;
            in_ptr += 4;
        }
    }
    
    return in_ptr - in_data;
}

static size_t svb_decode_quad_1234_scalar(uint8_t control, const uint8_t* in_data, uint32_t* out) {
    const uint8_t* in_ptr = in_data;
    
    for (int i = 0; i < 4; i++) {
        uint8_t code = (control >> (2 * i)) & 0x3;
        
        if (code == 0) {
            // 1 byte
            out[i] = *in_ptr;
            in_ptr += 1;
        } else if (code == 1) {
            // 2 bytes
            uint32_t val = 0;
            memcpy(&val, in_ptr, 2);
            out[i] = val;
            in_ptr += 2;
        } else if (code == 2) {
            // 3 bytes
            uint32_t val = 0;
            memcpy(&val, in_ptr, 3);
            out[i] = val;
            in_ptr += 3;
        } else { // code == 3
            // 4 bytes
            uint32_t val = 0;
            memcpy(&val, in_ptr, 4);
            out[i] = val;
            in_ptr += 4;
        }
    }
    
    return in_ptr - in_data;
}

// SSE4.1 SIMD implementations
// Requires: at least 16 bytes available at in_data
static size_t svb_decode_quad_0124_sse41(uint8_t control, const uint8_t* in_data, uint32_t* out) {
    // Load 16 bytes of input data
    __m128i data = _mm_loadu_si128((const __m128i*)in_data);
    
    // Load shuffle mask for this control byte
    __m128i shuffle_mask = _mm_loadu_si128((const __m128i*)shuffle_table_0124[control]);
    
    // Apply shuffle to rearrange bytes
    __m128i result = _mm_shuffle_epi8(data, shuffle_mask);
    
    // Store result
    _mm_storeu_si128((__m128i*)out, result);
    
    // Return number of bytes consumed
    return length_table_0124[control];
}

// Requires: at least 16 bytes available at in_data
static size_t svb_decode_quad_1234_sse41(uint8_t control, const uint8_t* in_data, uint32_t* out) {
    // Load 16 bytes of input data
    __m128i data = _mm_loadu_si128((const __m128i*)in_data);
    
    // Load shuffle mask for this control byte
    __m128i shuffle_mask = _mm_loadu_si128((const __m128i*)shuffle_table_1234[control]);
    
    // Apply shuffle to rearrange bytes
    __m128i result = _mm_shuffle_epi8(data, shuffle_mask);
    
    // Store result
    _mm_storeu_si128((__m128i*)out, result);
    
    // Return number of bytes consumed
    return length_table_1234[control];
}

// Function pointer types
typedef size_t (*decode_quad_func_t)(uint8_t control, const uint8_t* in_data, uint32_t* out);

// IFUNC resolvers
static decode_quad_func_t resolve_decode_0124(void) {
    if (has_sse41()) {
        return svb_decode_quad_0124_sse41;
    } else {
        return svb_decode_quad_0124_scalar;
    }
}

static decode_quad_func_t resolve_decode_1234(void) {
    if (has_sse41()) {
        return svb_decode_quad_1234_sse41;
    } else {
        return svb_decode_quad_1234_scalar;
    }
}

// GNU IFUNC attributes
size_t svb_decode_quad_0124(uint8_t control, const uint8_t* in_data, uint32_t* out)
    __attribute__((ifunc("resolve_decode_0124")));

size_t svb_decode_quad_1234(uint8_t control, const uint8_t* in_data, uint32_t* out)
    __attribute__((ifunc("resolve_decode_1234")));