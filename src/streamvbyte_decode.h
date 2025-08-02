#ifndef STREAMVBYTE_DECODE_H
#define STREAMVBYTE_DECODE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Decode a quad (4 integers) using StreamVByte 0124 variant
// 0124 means: 0 bytes for zero, 1 byte for <256, 2 bytes for <65536, 4 bytes otherwise
// Requires: in_data must be padded so that at least 16 bytes starting at in_data are readable
// Returns number of bytes consumed from in_data
size_t svb_decode_quad_0124(uint8_t control, const uint8_t* in_data, uint32_t* out);

// Decode a quad (4 integers) using StreamVByte 1234 variant  
// 1234 means: 1 byte for <256, 2 bytes for <65536, 3 bytes for <16M, 4 bytes otherwise
// Requires: in_data must be padded so that at least 16 bytes starting at in_data are readable
// Returns number of bytes consumed from in_data
size_t svb_decode_quad_1234(uint8_t control, const uint8_t* in_data, uint32_t* out);

#ifdef __cplusplus
}
#endif

#endif // STREAMVBYTE_DECODE_H