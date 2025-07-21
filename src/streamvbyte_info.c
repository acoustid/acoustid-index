#include "streamvbyte_block.h"
#include <stdio.h>

void print_streamvbyte_info() {
#if defined(__x86_64__) || defined(_M_X64)
    printf("StreamVByte: x86_64 platform - GCC function multiversioning enabled\n");
    printf("  - SSE4.1 optimized version available\n");
    printf("  - Generic fallback version available\n");
    printf("  - Runtime dispatch will select optimal version\n");
#elif defined(__aarch64__) || defined(_M_ARM64)
    printf("StreamVByte: ARM64 platform - GCC function multiversioning enabled\n");
    printf("  - NEON optimized version available\n");
    printf("  - Generic fallback version available\n");
    printf("  - Runtime dispatch will select optimal version\n");
#else
    printf("StreamVByte: Generic platform - Generic implementation only\n");
    printf("  - Using optimized generic StreamVByte implementation\n");
    printf("  - No SIMD optimizations available on this platform\n");
#endif
}
