#include <stdio.h>
#include <stdint.h>

// Generate StreamVByte shuffle tables for encoding and decoding
int main() {
    printf("// AUTO-GENERATED StreamVByte lookup tables\n");
    printf("// DO NOT EDIT - regenerate with generate_streamvbyte_tables.c\n\n");
    
    // Generate encoding shuffle table
    printf("// Generated shuffle table for StreamVByte encoding (PSHUFB/VTBL) - flattened\n");
    printf("static const uint8_t encode_shuffle_table[4096] = {\n");
    
    for (int control_byte = 0; control_byte < 256; control_byte++) {
        uint8_t cb = (uint8_t)control_byte;
        
        // Extract bytes needed for each of 4 values
        uint8_t bytes0 = ((cb >> 0) & 3) + 1;
        uint8_t bytes1 = ((cb >> 2) & 3) + 1;
        uint8_t bytes2 = ((cb >> 4) & 3) + 1;
        uint8_t bytes3 = ((cb >> 6) & 3) + 1;
        
        uint8_t pos = 0;
        uint8_t shuffle[16];
        
        // Initialize with 255 (invalid)
        for (int i = 0; i < 16; i++) {
            shuffle[i] = 255;
        }
        
        // Value 0 (bytes 0-3)
        for (int i = 0; i < bytes0; i++) {
            shuffle[pos++] = i;
        }
        
        // Value 1 (bytes 4-7)  
        for (int i = 0; i < bytes1; i++) {
            shuffle[pos++] = 4 + i;
        }
        
        // Value 2 (bytes 8-11)
        for (int i = 0; i < bytes2; i++) {
            shuffle[pos++] = 8 + i;
        }
        
        // Value 3 (bytes 12-15)
        for (int i = 0; i < bytes3; i++) {
            shuffle[pos++] = 12 + i;
        }
        
        printf("    ");
        
        for (int i = 0; i < 16; i++) {
            int element_idx = control_byte * 16 + i;
            printf("%d, ", shuffle[i]);
        }
        
        printf("  // 0x%02x\n", control_byte);
    }
    
    printf("\n};\n\n");
    
    // Generate decoding shuffle table
    printf("// Generated shuffle table for StreamVByte decoding (VTBL) - flattened\n");
    printf("static const uint8_t decode_shuffle_table[4096] = {\n");
    
    for (int control_byte = 0; control_byte < 256; control_byte++) {
        uint8_t cb = (uint8_t)control_byte;
        
        // Extract bytes needed for each of 4 values
        uint8_t bytes0 = ((cb >> 0) & 3) + 1;
        uint8_t bytes1 = ((cb >> 2) & 3) + 1;
        uint8_t bytes2 = ((cb >> 4) & 3) + 1;
        uint8_t bytes3 = ((cb >> 6) & 3) + 1;
        
        uint8_t shuffle[16];
        uint8_t packed_pos = 0;
        
        // Initialize with 255 (will become 0 when used with vtbl)
        for (int i = 0; i < 16; i++) {
            shuffle[i] = 255;
        }
        
        // Map packed bytes to their 32-bit positions
        // Value 0 goes to positions 0,1,2,3
        for (int i = 0; i < bytes0; i++) {
            shuffle[i] = packed_pos++;
        }
        
        // Value 1 goes to positions 4,5,6,7
        for (int i = 0; i < bytes1; i++) {
            shuffle[4 + i] = packed_pos++;
        }
        
        // Value 2 goes to positions 8,9,10,11
        for (int i = 0; i < bytes2; i++) {
            shuffle[8 + i] = packed_pos++;
        }
        
        // Value 3 goes to positions 12,13,14,15
        for (int i = 0; i < bytes3; i++) {
            shuffle[12 + i] = packed_pos++;
        }
        
        printf("    ");

        for (int i = 0; i < 16; i++) {
            int element_idx = control_byte * 16 + i;
            printf("%d, ", shuffle[i]);
        }
        
        printf("  // 0x%02x\n", control_byte);
    }
    
    printf("\n};\n\n");
    
    // Generate length table
    printf("// Generated length table for StreamVByte\n");
    printf("static const uint8_t length_table[256] = {\n");
    
    for (int control_byte = 0; control_byte < 256; control_byte++) {
        uint8_t cb = (uint8_t)control_byte;
        
        // Calculate total length for this control byte
        uint8_t bytes0 = ((cb >> 0) & 3) + 1;
        uint8_t bytes1 = ((cb >> 2) & 3) + 1;
        uint8_t bytes2 = ((cb >> 4) & 3) + 1;
        uint8_t bytes3 = ((cb >> 6) & 3) + 1;
        uint8_t total_length = bytes0 + bytes1 + bytes2 + bytes3;
        
        if (control_byte % 16 == 0) {
            if (control_byte > 0) printf("\n");
            printf("    ");
        }
        
        printf("%2d", total_length);
        if (control_byte < 255) printf(", ");
    }
    
    printf("\n};\n");
    
    return 0;
}
