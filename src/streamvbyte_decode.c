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
    __m128i shuffle_mask = _mm_load_si128((const __m128i*)&shuffle_table_0124[control]);
    
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
    __m128i shuffle_mask = _mm_load_si128((const __m128i*)&shuffle_table_1234[control]);
    
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

// Scalar delta decoding implementation
static void svb_delta_decode_scalar(const uint32_t* input, uint32_t* output, size_t count, uint32_t first_value) {
    if (count == 0) return;
    
    if (input == output) {
        // In-place operation
        output[0] += first_value;
        for (size_t i = 1; i < count; i++) {
            output[i] += output[i - 1];
        }
    } else {
        // Separate input/output arrays
        output[0] = input[0] + first_value;
        for (size_t i = 1; i < count; i++) {
            output[i] = input[i] + output[i - 1];
        }
    }
}

// SSE4.1 SIMD delta decoding implementation
static void svb_delta_decode_sse41(const uint32_t* input, uint32_t* output, size_t count, uint32_t first_value) {
    if (count == 0) return;
    
    if (input == output) {
        // In-place operation
        output[0] += first_value;
        
        if (count == 1) return;
        
        uint32_t carry = output[0];
        size_t i = 1;
        
        // Process 4 elements at a time with SIMD
        while (i + 3 < count) {
            // Load 4 input values
            __m128i vec = _mm_loadu_si128((const __m128i*)&output[i]);
            
            // Compute prefix sum within the vector FIRST: [a, b, c, d] -> [a, a+b, a+b+c, a+b+c+d]
            // Step 1: [a, b, c, d] + [0, a, b, c] = [a, a+b, b+c, c+d]
            __m128i temp1 = _mm_slli_si128(vec, 4);
            vec = _mm_add_epi32(vec, temp1);
            
            // Step 2: [a, a+b, b+c, c+d] + [0, 0, a, a+b] = [a, a+b, a+b+c, a+b+c+d]
            __m128i temp2 = _mm_slli_si128(vec, 8);
            vec = _mm_add_epi32(vec, temp2);
            
            // THEN add carry to all elements: [a, a+b, a+b+c, a+b+c+d] + [carry, carry, carry, carry]
            __m128i carry_vec = _mm_set1_epi32(carry);
            vec = _mm_add_epi32(vec, carry_vec);
            
            // Store result
            _mm_storeu_si128((__m128i*)&output[i], vec);
            
            // Extract last element as new carry
            carry = _mm_extract_epi32(vec, 3);
            i += 4;
        }
        
        // Handle remaining elements (1-3) with scalar code
        while (i < count) {
            output[i] += carry;
            carry = output[i];
            i++;
        }
    } else {
        // Separate input/output arrays
        output[0] = input[0] + first_value;
        
        if (count == 1) return;
        
        uint32_t carry = output[0];
        size_t i = 1;
        
        // Process 4 elements at a time with SIMD
        while (i + 3 < count) {
            // Load 4 input values
            __m128i vec = _mm_loadu_si128((const __m128i*)&input[i]);
            
            // Compute prefix sum within the vector FIRST: [a, b, c, d] -> [a, a+b, a+b+c, a+b+c+d]
            // Step 1: [a, b, c, d] + [0, a, b, c] = [a, a+b, b+c, c+d]
            __m128i temp1 = _mm_slli_si128(vec, 4);
            vec = _mm_add_epi32(vec, temp1);
            
            // Step 2: [a, a+b, b+c, c+d] + [0, 0, a, a+b] = [a, a+b, a+b+c, a+b+c+d]
            __m128i temp2 = _mm_slli_si128(vec, 8);
            vec = _mm_add_epi32(vec, temp2);
            
            // THEN add carry to all elements: [a, a+b, a+b+c, a+b+c+d] + [carry, carry, carry, carry]
            __m128i carry_vec = _mm_set1_epi32(carry);
            vec = _mm_add_epi32(vec, carry_vec);
            
            // Store result
            _mm_storeu_si128((__m128i*)&output[i], vec);
            
            // Extract last element as new carry
            carry = _mm_extract_epi32(vec, 3);
            i += 4;
        }
        
        // Handle remaining elements (1-3) with scalar code
        while (i < count) {
            output[i] = input[i] + carry;
            carry = output[i];
            i++;
        }
    }
}

// Function pointer type
typedef void (*delta_decode_func_t)(const uint32_t* input, uint32_t* output, size_t count, uint32_t first_value);

// IFUNC resolver for delta decoding
static delta_decode_func_t resolve_delta_decode(void) {
    if (has_sse41()) {
        return svb_delta_decode_sse41;
    } else {
        return svb_delta_decode_scalar;
    }
}

// GNU IFUNC attributes
size_t svb_decode_quad_0124(uint8_t control, const uint8_t* in_data, uint32_t* out)
    __attribute__((ifunc("resolve_decode_0124")));

size_t svb_decode_quad_1234(uint8_t control, const uint8_t* in_data, uint32_t* out)
    __attribute__((ifunc("resolve_decode_1234")));

// In-place delta decoding - scalar implementation
static void svb_delta_decode_inplace_scalar(uint32_t* data, size_t count, uint32_t first_value) {
    if (count == 0) return;
    
    data[0] += first_value;
    for (size_t i = 1; i < count; i++) {
        data[i] += data[i - 1];
    }
}

// In-place delta decoding - SSE4.1 SIMD implementation
static void svb_delta_decode_inplace_sse41(uint32_t* data, size_t count, uint32_t first_value) {
    if (count == 0) return;
    
    data[0] += first_value;
    if (count == 1) return;
    
    uint32_t carry = data[0];
    size_t i = 1;
    
    // Process 4 elements at a time with SIMD
    while (i + 3 < count) {
        // Load 4 values
        __m128i vec = _mm_loadu_si128((const __m128i*)&data[i]);
        
        // Compute prefix sum within the vector FIRST: [a, b, c, d] -> [a, a+b, a+b+c, a+b+c+d]
        // Step 1: [a, b, c, d] + [0, a, b, c] = [a, a+b, b+c, c+d]
        __m128i temp1 = _mm_slli_si128(vec, 4);
        vec = _mm_add_epi32(vec, temp1);
        
        // Step 2: [a, a+b, b+c, c+d] + [0, 0, a, a+b] = [a, a+b, a+b+c, a+b+c+d]
        __m128i temp2 = _mm_slli_si128(vec, 8);
        vec = _mm_add_epi32(vec, temp2);
        
        // THEN add carry to all elements: [a, a+b, a+b+c, a+b+c+d] + [carry, carry, carry, carry]
        __m128i carry_vec = _mm_set1_epi32(carry);
        vec = _mm_add_epi32(vec, carry_vec);
        
        // Store result
        _mm_storeu_si128((__m128i*)&data[i], vec);
        
        // Extract last element as new carry
        carry = _mm_extract_epi32(vec, 3);
        i += 4;
    }
    
    // Handle remaining elements (1-3) with scalar code
    while (i < count) {
        data[i] += carry;
        carry = data[i];
        i++;
    }
}

// Function pointer type for in-place delta decoding
typedef void (*delta_decode_inplace_func_t)(uint32_t* data, size_t count, uint32_t first_value);

// IFUNC resolver for in-place delta decoding
static delta_decode_inplace_func_t resolve_delta_decode_inplace(void) {
    if (has_sse41()) {
        return svb_delta_decode_inplace_sse41;
    } else {
        return svb_delta_decode_inplace_scalar;
    }
}

void svb_delta_decode(const uint32_t* input, uint32_t* output, size_t count, uint32_t first_value)
    __attribute__((ifunc("resolve_delta_decode")));

void svb_delta_decode_inplace(uint32_t* data, size_t count, uint32_t first_value)
    __attribute__((ifunc("resolve_delta_decode_inplace")));