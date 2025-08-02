#!/usr/bin/env python3
"""
Generate shuffle tables for StreamVByte x64 SIMD decoding.
Generates both 0124 and 1234 variants.
"""

def generate_length_table_0124():
    """Generate length table for 0124 variant (0,1,2,4 bytes per value)"""
    lengths = []
    for control_byte in range(256):
        total_len = 0
        for i in range(4):
            code = (control_byte >> (2 * i)) & 0x3
            if code == 0:
                total_len += 0  # 0 bytes
            elif code == 1:
                total_len += 1  # 1 byte
            elif code == 2:
                total_len += 2  # 2 bytes
            elif code == 3:
                total_len += 4  # 4 bytes
        lengths.append(total_len)
    return lengths

def generate_length_table_1234():
    """Generate length table for 1234 variant (1,2,3,4 bytes per value)"""
    lengths = []
    for control_byte in range(256):
        total_len = 0
        for i in range(4):
            code = (control_byte >> (2 * i)) & 0x3
            if code == 0:
                total_len += 1  # 1 byte
            elif code == 1:
                total_len += 2  # 2 bytes
            elif code == 2:
                total_len += 3  # 3 bytes
            elif code == 3:
                total_len += 4  # 4 bytes
        lengths.append(total_len)
    return lengths

def generate_shuffle_table_0124():
    """Generate shuffle table for 0124 variant"""
    shuffle_table = []
    
    for control_byte in range(256):
        shuffle_mask = [-1] * 16  # Initialize with -1 (0xFF)
        input_pos = 0
        
        for i in range(4):
            code = (control_byte >> (2 * i)) & 0x3
            output_base = i * 4  # Each output integer takes 4 bytes
            
            if code == 0:
                # 0 bytes - leave as zeros (shuffle mask stays -1)
                pass
            elif code == 1:
                # 1 byte
                shuffle_mask[output_base] = input_pos
                input_pos += 1
            elif code == 2:
                # 2 bytes
                shuffle_mask[output_base] = input_pos
                shuffle_mask[output_base + 1] = input_pos + 1
                input_pos += 2
            elif code == 3:
                # 4 bytes
                shuffle_mask[output_base] = input_pos
                shuffle_mask[output_base + 1] = input_pos + 1
                shuffle_mask[output_base + 2] = input_pos + 2
                shuffle_mask[output_base + 3] = input_pos + 3
                input_pos += 4
        
        shuffle_table.append(shuffle_mask)
    
    return shuffle_table

def generate_shuffle_table_1234():
    """Generate shuffle table for 1234 variant"""
    shuffle_table = []
    
    for control_byte in range(256):
        shuffle_mask = [-1] * 16  # Initialize with -1 (0xFF)
        input_pos = 0
        
        for i in range(4):
            code = (control_byte >> (2 * i)) & 0x3
            output_base = i * 4  # Each output integer takes 4 bytes
            
            if code == 0:
                # 1 byte
                shuffle_mask[output_base] = input_pos
                input_pos += 1
            elif code == 1:
                # 2 bytes
                shuffle_mask[output_base] = input_pos
                shuffle_mask[output_base + 1] = input_pos + 1
                input_pos += 2
            elif code == 2:
                # 3 bytes
                shuffle_mask[output_base] = input_pos
                shuffle_mask[output_base + 1] = input_pos + 1
                shuffle_mask[output_base + 2] = input_pos + 2
                input_pos += 3
            elif code == 3:
                # 4 bytes
                shuffle_mask[output_base] = input_pos
                shuffle_mask[output_base + 1] = input_pos + 1
                shuffle_mask[output_base + 2] = input_pos + 2
                shuffle_mask[output_base + 3] = input_pos + 3
                input_pos += 4
        
        shuffle_table.append(shuffle_mask)
    
    return shuffle_table

def format_shuffle_table(shuffle_table, variant_name):
    """Format shuffle table as C array"""
    lines = [f"static const int8_t shuffle_table_{variant_name}[256][16] __attribute__((aligned(16))) = {{"]
    
    for i, mask in enumerate(shuffle_table):
        # Format the mask values
        mask_str = ", ".join(f"{v:3d}" if v >= 0 else " -1" for v in mask)
        comment = f"// {i:02X}: "
        
        # Add binary representation comment
        binary_parts = []
        for j in range(4):
            code = (i >> (2 * j)) & 0x3
            binary_parts.append(f"{code}")
        comment += "".join(reversed(binary_parts))
        
        lines.append(f"    {{ {mask_str} }}, {comment}")
    
    lines.append("};")
    return "\n".join(lines)

def format_length_table(length_table, variant_name):
    """Format length table as C array"""
    lines = [f"static const uint8_t length_table_{variant_name}[256] = {{"]
    
    # Format 16 values per line
    for i in range(0, 256, 16):
        values = length_table[i:i+16]
        values_str = ", ".join(f"{v:2d}" for v in values)
        lines.append(f"    {values_str},")
    
    lines.append("};")
    return "\n".join(lines)

def generate_header_file(shuffle_0124, shuffle_1234, length_0124, length_1234):
    """Generate complete header file with both tables"""
    header = """#ifndef STREAMVBYTE_TABLES_H
#define STREAMVBYTE_TABLES_H

#include <stdint.h>

// Shuffle tables for StreamVByte SIMD decoding
// Generated automatically by generate_shuffle_tables.py

"""
    
    header += format_shuffle_table(shuffle_0124, "0124") + "\n\n"
    header += format_shuffle_table(shuffle_1234, "1234") + "\n\n"
    header += format_length_table(length_0124, "0124") + "\n\n"
    header += format_length_table(length_1234, "1234") + "\n\n"
    
    header += "#endif // STREAMVBYTE_TABLES_H\n"
    
    return header

def main():
    print("Generating StreamVByte shuffle tables...")
    
    # Generate all tables
    shuffle_0124 = generate_shuffle_table_0124()
    shuffle_1234 = generate_shuffle_table_1234()
    length_0124 = generate_length_table_0124()
    length_1234 = generate_length_table_1234()
    
    # Generate header file
    header_content = generate_header_file(shuffle_0124, shuffle_1234, length_0124, length_1234)
    
    # Write to file
    with open("src/streamvbyte_tables.h", "w") as f:
        f.write(header_content)
    
    print("Generated src/streamvbyte_tables.h")
    
    # Verify a few entries
    print("\nVerification:")
    print(f"Control byte 0x00 (0000): length_0124={length_0124[0]}, length_1234={length_1234[0]}")
    print(f"Control byte 0xFF (3333): length_0124={length_0124[255]}, length_1234={length_1234[255]}")
    print(f"Control byte 0x55 (1111): length_0124={length_0124[0x55]}, length_1234={length_1234[0x55]}")

if __name__ == "__main__":
    main()