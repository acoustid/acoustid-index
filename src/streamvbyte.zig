const std = @import("std");
const builtin = @import("builtin");
const Item = @import("segment.zig").Item;

// Padding required for SIMD decode functions to safely read 16 bytes
pub const SIMD_DECODE_PADDING = 16;

// StreamVByte encoding/decoding functions for compressing integer lists

const Vu8x16 = @Vector(16, u8);
const Vu32x4 = @Vector(4, u32);

// CPU feature detection
const has_ssse3 = switch (builtin.cpu.arch) {
    .x86_64, .x86 => std.Target.x86.featureSetHas(builtin.cpu.features, .ssse3),
    else => false,
};

const has_sse41 = switch (builtin.cpu.arch) {
    .x86_64, .x86 => std.Target.x86.featureSetHas(builtin.cpu.features, .sse4_1),
    else => false,
};

// Backend detection - only use LLVM intrinsics with LLVM backend
const use_llvm_intrinsics = switch (builtin.zig_backend) {
    .stage2_llvm => true,
    else => false,
};
const use_inline_asm = true;

// StreamVByte shuffle implementation with multi-tier fallback
// Uses pshufb/vpshufb behavior: high bit set in mask -> output 0
fn shuffle(x: Vu8x16, m: Vu8x16) Vu8x16 {
    if (use_llvm_intrinsics and has_ssse3) {
        // Use LLVM intrinsic - compiles to single pshufb/vpshufb
        const builtin_fn = struct {
            extern fn @"llvm.x86.ssse3.pshuf.b.128"(Vu8x16, Vu8x16) Vu8x16;
        }.@"llvm.x86.ssse3.pshuf.b.128";
        return builtin_fn(x, m);
    } else if (use_inline_asm and has_ssse3) {
        // Use inline assembly fallback
        var result = x;
        asm ("pshufb %[mask], %[result]"
            : [result] "+x" (result),
            : [mask] "x" (m),
        );
        return result;
    } else {
        // Safe scalar fallback - implements exact Intel pshufb specification
        var r: Vu8x16 = undefined;
        inline for (0..16) |i| {
            if ((m[i] & 0x80) != 0) { // Check bit 7
                r[i] = 0;
            } else {
                const index = m[i] & 0x0F; // Use only low 4 bits
                r[i] = x[index];
            }
        }
        return r;
    }
}

// Variant enum for StreamVByte decoding
pub const Variant = enum {
    variant0124,
    variant0124_minus1, // 0/1/2/4 variant with value-1 for counts (0 bytes for count=1)
    variant1234,
};

// Delta decoding mode enum
pub const DeltaMode = enum {
    no_delta,
    delta,
};

// Shuffle tables for 0124 variant (0 bytes for zero, 1 byte for <256, 2 bytes for <65536, 4 bytes otherwise)
const shuffle_table_0124: [256]Vu8x16 = blk: {
    @setEvalBranchQuota(10000);
    break :blk initShuffleTable0124();
};

// Shuffle tables for 1234 variant (1 byte for <256, 2 bytes for <65536, 3 bytes for <16M, 4 bytes otherwise)
const shuffle_table_1234: [256]Vu8x16 = blk: {
    @setEvalBranchQuota(10000);
    break :blk initShuffleTable1234();
};

// Length tables for each control byte
const length_table_0124: [256]u8 = blk: {
    @setEvalBranchQuota(10000);
    break :blk initLengthTable0124();
};

const length_table_1234: [256]u8 = blk: {
    @setEvalBranchQuota(10000);
    break :blk initLengthTable1234();
};

// Initialize shuffle table for 0124 variant at comptime
fn initShuffleTable0124() [256]Vu8x16 {
    var table: [256]Vu8x16 = undefined;
    for (0..256) |control| {
        var mask: Vu8x16 = @splat(@as(u8, 0x80));
        var offset: usize = 0;
        for (0..4) |i| {
            const code: u2 = @intCast((control >> (2 * i)) & 0x3);
            const pos = i * 4;

            switch (code) {
                0 => { // 0 bytes (zero)
                    // All bytes remain -1 (zero)
                },
                1 => { // 1 byte
                    mask[pos] = @intCast(offset);
                    offset += 1;
                },
                2 => { // 2 bytes
                    mask[pos] = @intCast(offset);
                    mask[pos + 1] = @intCast(offset + 1);
                    offset += 2;
                },
                3 => { // 4 bytes
                    mask[pos] = @intCast(offset);
                    mask[pos + 1] = @intCast(offset + 1);
                    mask[pos + 2] = @intCast(offset + 2);
                    mask[pos + 3] = @intCast(offset + 3);
                    offset += 4;
                },
            }
        }
        table[control] = mask;
    }
    return table;
}

// Initialize shuffle table for 1234 variant at comptime
fn initShuffleTable1234() [256]Vu8x16 {
    var table: [256]Vu8x16 = undefined;
    for (0..256) |control| {
        var mask: Vu8x16 = @splat(@as(u8, 0x80));
        var offset: usize = 0;
        for (0..4) |i| {
            const code: u2 = @intCast((control >> (2 * i)) & 0x3);
            const pos = i * 4;

            switch (code) {
                0 => { // 1 byte
                    mask[pos] = @intCast(offset);
                    offset += 1;
                },
                1 => { // 2 bytes
                    mask[pos] = @intCast(offset);
                    mask[pos + 1] = @intCast(offset + 1);
                    offset += 2;
                },
                2 => { // 3 bytes
                    mask[pos] = @intCast(offset);
                    mask[pos + 1] = @intCast(offset + 1);
                    mask[pos + 2] = @intCast(offset + 2);
                    offset += 3;
                },
                3 => { // 4 bytes
                    mask[pos] = @intCast(offset);
                    mask[pos + 1] = @intCast(offset + 1);
                    mask[pos + 2] = @intCast(offset + 2);
                    mask[pos + 3] = @intCast(offset + 3);
                    offset += 4;
                },
            }
        }
        table[control] = mask;
    }
    return table;
}

// Initialize length table for 0124 variant at comptime
fn initLengthTable0124() [256]u8 {
    var table: [256]u8 = undefined;
    for (0..256) |control| {
        var total: u8 = 0;
        for (0..4) |i| {
            const code: u2 = @intCast((control >> (2 * i)) & 0x3);
            switch (code) {
                0 => total += 0,
                1 => total += 1,
                2 => total += 2,
                3 => total += 4,
            }
        }
        table[control] = total;
    }
    return table;
}

// Initialize length table for 1234 variant at comptime
fn initLengthTable1234() [256]u8 {
    var table: [256]u8 = undefined;
    for (0..256) |control| {
        var total: u8 = 0;
        for (0..4) |i| {
            const code: u2 = @intCast((control >> (2 * i)) & 0x3);
            switch (code) {
                0 => total += 1,
                1 => total += 2,
                2 => total += 3,
                3 => total += 4,
            }
        }
        table[control] = total;
    }
    return table;
}

// Base function for decoding a quad (4 integers) using StreamVByte with SIMD acceleration
// Requires: in_data must be padded so that at least 16 bytes starting at in_data are readable
// Returns the decoded vector and number of bytes consumed from in_data
inline fn svbDecodeQuadBase(comptime variant: Variant, control: u8, in_data: []const u8) struct { values: Vu32x4, consumed: usize } {
    std.debug.assert(in_data.len >= SIMD_DECODE_PADDING); // SIMD implementation requires padding

    // Load 16 bytes of input data
    const data: Vu8x16 = in_data[0..16].*;

    // Load shuffle mask and length table based on variant
    const shuffle_table = switch (variant) {
        .variant0124 => shuffle_table_0124,
        .variant0124_minus1 => shuffle_table_0124,
        .variant1234 => shuffle_table_1234,
    };
    const length_table = switch (variant) {
        .variant0124 => length_table_0124,
        .variant0124_minus1 => length_table_0124,
        .variant1234 => length_table_1234,
    };

    // Apply shuffle to rearrange bytes
    const result = shuffle(data, shuffle_table[control]);

    // Convert result to 4 u32 values
    var result_u32: Vu32x4 = @bitCast(result);

    // For variant0124_minus1, add 1 to each decoded value
    if (variant == .variant0124_minus1) {
        const one_vec: Vu32x4 = @splat(1);
        result_u32 = result_u32 + one_vec;
    }

    return .{ .values = result_u32, .consumed = length_table[control] };
}

// Decode a quad (4 integers) using StreamVByte with SIMD acceleration
// Requires: in_data must be padded so that at least 16 bytes starting at in_data are readable
// Returns number of bytes consumed from in_data
inline fn svbDecodeQuad(comptime variant: Variant, control: u8, in_data: []const u8, out: []u32) usize {
    std.debug.assert(out.len >= 4);

    const result = svbDecodeQuadBase(variant, control, in_data);
    out[0..4].* = result.values;
    return result.consumed;
}

// Decode a quad (4 integers) using StreamVByte with SIMD acceleration and fused delta decoding
// Requires: in_data must be padded so that at least 16 bytes starting at in_data are readable
// Returns number of bytes consumed from in_data
// Performs delta decoding in-place using the same SIMD registers for maximum efficiency
inline fn svbDecodeQuadWithDelta(comptime variant: Variant, control: u8, in_data: []const u8, out: []u32, carry: u32) usize {
    std.debug.assert(out.len >= 4);

    const result = svbDecodeQuadBase(variant, control, in_data);
    var values = result.values;

    // Perform fused delta decoding on the SIMD registers
    // Compute prefix sum within the vector: [a, b, c, d] -> [a, a+b, a+b+c, a+b+c+d]
    // Step 1: [a, b, c, d] + [0, a, b, c] = [a, a+b, b+c, c+d]
    values += std.simd.shiftElementsRight(values, 1, 0);
    // Step 2: [a, a+b, b+c, c+d] + [0, 0, a, a+b] = [a, a+b, a+b+c, a+b+c+d]
    values += std.simd.shiftElementsRight(values, 2, 0);

    // Add carry to all elements
    const carry_vec: Vu32x4 = @splat(carry);
    values = values + carry_vec;

    out[0..4].* = values;
    return result.consumed;
}

// Apply delta decoding in-place with SIMD acceleration
// Computes prefix sum in-place: data[i] += data[i-1] for i > 0, data[0] += first_value
pub fn svbDeltaDecodeInPlace(data: []u32, first_value: u32) void {
    if (has_sse41) {
        svbDeltaDecodeInPlaceSSE41(data, first_value);
    } else {
        // Scalar fallback
        if (data.len == 0) return;
        data[0] += first_value;
        for (1..data.len) |i| {
            data[i] += data[i - 1];
        }
    }
}

// SIMD-accelerated delta decode using SSE4.1 intrinsics
fn svbDeltaDecodeInPlaceSSE41(data: []u32, first_value: u32) void {
    if (data.len == 0) return;

    data[0] += first_value;
    if (data.len == 1) return;

    var carry = data[0];
    var i: usize = 1;

    // Process 4 elements at a time with SIMD
    while (i + 3 < data.len) {
        // Load 4 values
        var vec: Vu32x4 = data[i..][0..4].*;

        // Compute prefix sum within the vector FIRST: [a, b, c, d] -> [a, a+b, a+b+c, a+b+c+d]
        // Step 1: [a, b, c, d] + [0, a, b, c] = [a, a+b, b+c, c+d]
        vec += std.simd.shiftElementsRight(vec, 1, 0);
        // Step 2: [a, a+b, b+c, c+d] + [0, 0, a, a+b] = [a, a+b, a+b+c, a+b+c+d]
        vec += std.simd.shiftElementsRight(vec, 2, 0);

        // THEN add carry to all elements: [a, a+b, a+b+c, a+b+c+d] + [carry, carry, carry, carry]
        const carry_vec: Vu32x4 = @splat(carry);
        vec = vec + carry_vec;

        // Store result
        data[i..][0..4].* = vec;

        // Extract last element as new carry
        carry = vec[3];
        i += 4;
    }

    // Handle remaining elements (1-3) with scalar code
    while (i < data.len) {
        data[i] += carry;
        carry = data[i];
        i += 1;
    }
}

pub fn decodeValues(
    total_items: usize,
    start_item: usize,
    end_item: usize,
    in: []const u8,
    out: []u32,
    comptime variant: Variant,
    comptime delta_mode: DeltaMode,
    first_value: if (delta_mode == .delta) u32 else void,
) void {
    const length_table = switch (variant) {
        .variant0124 => length_table_0124,
        .variant1234 => length_table_1234,
        .variant0124_minus1 => length_table_0124, // Reuse 0124 tables
    };

    const start_quad = start_item / 4;
    const end_quad = (end_item + 3) / 4;
    const total_quads = (total_items + 3) / 4;

    // Skip to the starting quad by calculating data offset
    var data_offset: usize = total_quads;
    for (0..start_quad) |quad_idx| {
        data_offset += length_table[in[quad_idx]];
    }
    var in_control_ptr = in[start_quad..total_quads];
    var in_data_ptr = in[data_offset..];

    const aligned_start_item = start_quad * 4;
    const aligned_end_item = end_quad * 4;

    var out_ptr = out[aligned_start_item..aligned_end_item];
    var remaining: usize = end_quad - start_quad;

    if (delta_mode == .delta) {
        var carry = first_value;

        // Process quads with fused delta decoding
        while (remaining > 0) {
            const consumed = svbDecodeQuadWithDelta(variant, in_control_ptr[0], in_data_ptr, out_ptr, carry);

            // Update carry to the last decoded value for the next quad
            carry = out_ptr[3];

            in_control_ptr = in_control_ptr[1..];
            in_data_ptr = in_data_ptr[consumed..];
            out_ptr = out_ptr[4..];
            remaining -= 1;
        }
    } else {
        // Use the regular decoding implementation
        while (remaining >= 8) {
            const controls = std.mem.readInt(u64, in_control_ptr[0..8], .little);
            inline for (0..8) |i| {
                const control: u8 = @intCast((controls >> (8 * i)) & 0xFF);
                const consumed = svbDecodeQuad(variant, control, in_data_ptr, out_ptr[i * 4 ..]);
                in_data_ptr = in_data_ptr[consumed..];
            }
            in_control_ptr = in_control_ptr[8..];
            out_ptr = out_ptr[32..]; // 8 quads * 4 items per quad
            remaining -= 8;
        }

        while (remaining > 0) {
            const consumed = svbDecodeQuad(variant, in_control_ptr[0], in_data_ptr, out_ptr);
            in_control_ptr = in_control_ptr[1..];
            in_data_ptr = in_data_ptr[consumed..];
            out_ptr = out_ptr[4..];
            remaining -= 1;
        }
    }
}

// Encode single value into a StreamVByte encoded byte array.
/// Encodes a single 32-bit integer using the StreamVByte "0124" variant,
/// where the control byte uses bits to indicate the encoded size:
/// 0 bytes for zero, 1 byte for <256, 2 bytes for <65536, 4 bytes otherwise.
pub fn svbEncodeValue0124(in: u32, out_data: []u8, out_control: *u8, comptime index: u8) usize {
    if (in == 0) {
        out_control.* |= 0 << (2 * index);
        return 0;
    } else if (in < (1 << 8)) {
        std.mem.writeInt(u8, out_data[0..1], @intCast(in), .little);
        out_control.* |= 1 << (2 * index);
        return 1;
    } else if (in < (1 << 16)) {
        std.mem.writeInt(u16, out_data[0..2], @intCast(in), .little);
        out_control.* |= 2 << (2 * index);
        return 2;
    } else {
        std.mem.writeInt(u32, out_data[0..4], in, .little);
        out_control.* |= 3 << (2 * index);
        return 4;
    }
}

// Encodes a 32-bit integer into a StreamVByte format using the 1/2/3/4-byte variant.
// Control byte is updated to indicate the number of bytes used for encoding.
// Returns the number of bytes written to out_data.
pub fn svbEncodeValue1234(in: u32, out_data: []u8, out_control: *u8, comptime index: u8) usize {
    if (in < (1 << 8)) {
        std.mem.writeInt(u8, out_data[0..1], @intCast(in), .little);
        out_control.* |= 0 << (2 * index);
        return 1;
    } else if (in < (1 << 16)) {
        std.mem.writeInt(u16, out_data[0..2], @intCast(in), .little);
        out_control.* |= 1 << (2 * index);
        return 2;
    } else if (in < (1 << 24)) {
        std.mem.writeInt(u24, out_data[0..3], @intCast(in), .little);
        out_control.* |= 2 << (2 * index);
        return 3;
    } else {
        std.mem.writeInt(u32, out_data[0..4], in, .little);
        out_control.* |= 3 << (2 * index);
        return 4;
    }
}

// Encode four 32-bit integers into a StreamVByte encoded byte array. (0124 variant)
pub fn svbEncodeQuad0124(in: [4]u32, out_data: []u8, out_control: *u8) usize {
    var out_data_ptr = out_data;
    out_control.* = 0;
    inline for (0..4) |i| {
        const size = svbEncodeValue0124(in[i], out_data_ptr, out_control, i);
        out_data_ptr = out_data_ptr[size..];
    }
    return out_data.len - out_data_ptr.len;
}

// Encode four 32-bit integers into a StreamVByte encoded byte array. (1234 variant)
pub fn svbEncodeQuad1234(in: [4]u32, out_data: []u8, out_control: *u8) usize {
    var out_data_ptr = out_data;
    out_control.* = 0; // Reset control byte
    inline for (0..4) |i| {
        const size = svbEncodeValue1234(in[i], out_data_ptr, out_control, i);
        out_data_ptr = out_data_ptr[size..];
    }
    return out_data.len - out_data_ptr.len;
}

// Calculate the size needed to encode four 32-bit integers with StreamVByte (0124 variant)
pub fn svbEncodeQuadSize0124(in: [4]u32) usize {
    var size: usize = 0;
    inline for (0..4) |i| {
        const val = in[i];
        if (val == 0) {
            size += 0;
        } else if (val < (1 << 8)) {
            size += 1;
        } else if (val < (1 << 16)) {
            size += 2;
        } else {
            size += 4;
        }
    }
    return size;
}

// Calculate the size needed to encode four 32-bit integers with StreamVByte (1234 variant)
pub fn svbEncodeQuadSize1234(in: [4]u32) usize {
    var size: usize = 0;
    inline for (0..4) |i| {
        const val = in[i];
        if (val < (1 << 8)) {
            size += 1;
        } else if (val < (1 << 16)) {
            size += 2;
        } else if (val < (1 << 24)) {
            size += 3;
        } else {
            size += 4;
        }
    }
    return size;
}

test "shuffle" {
    const data: Vu8x16 = .{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    const mask: Vu8x16 = .{ 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };
    const result = shuffle(data, mask);
    const expected: Vu8x16 = .{ 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };
    try std.testing.expectEqual(expected, result);
}

test "svbDecodeQuad0124 SIMD" {
    // Test simple case: [1, 2, 0, 4] with control byte
    // 1 (1 byte), 2 (1 byte), 0 (0 bytes), 4 (1 byte)
    // Control bits: 01 01 00 01 = codes [1, 1, 0, 1]
    const control: u8 = 0b01_00_01_01;
    const input = [_]u8{ 1, 2, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    var output: [4]u32 = undefined;

    const consumed = svbDecodeQuad(.variant0124, control, &input, &output);
    try std.testing.expectEqual(3, consumed);
    try std.testing.expectEqual(@as(u32, 1), output[0]);
    try std.testing.expectEqual(@as(u32, 2), output[1]);
    try std.testing.expectEqual(@as(u32, 0), output[2]);
    try std.testing.expectEqual(@as(u32, 4), output[3]);
}

test "svbDecodeQuad1234 SIMD" {
    // Test simple case: [1, 2, 3, 4]
    // 1 (1 byte), 2 (1 byte), 3 (1 byte), 4 (1 byte)
    // Control bits: 00 00 00 00 = codes [0, 0, 0, 0] = [1byte, 1byte, 1byte, 1byte]
    const control: u8 = 0b00_00_00_00;
    const input = [_]u8{ 1, 2, 3, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    var output: [4]u32 = undefined;

    const consumed = svbDecodeQuad(.variant1234, control, &input, &output);
    try std.testing.expectEqual(4, consumed);
    try std.testing.expectEqual(@as(u32, 1), output[0]);
    try std.testing.expectEqual(@as(u32, 2), output[1]);
    try std.testing.expectEqual(@as(u32, 3), output[2]);
    try std.testing.expectEqual(@as(u32, 4), output[3]);
}

test "svbDecodeQuad with fused delta decoding" {
    // Test fused delta decoding: input [10, 5, 3, 2] with carry 100
    // Expected output: [110, 115, 118, 120] (prefix sums with carry)
    const control: u8 = 0b00_00_00_00; // All 1-byte values
    const input = [_]u8{ 10, 5, 3, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    var output: [4]u32 = undefined;

    const consumed = svbDecodeQuadWithDelta(.variant1234, control, &input, &output, 100);
    try std.testing.expectEqual(4, consumed);
    try std.testing.expectEqual(@as(u32, 110), output[0]); // 100 + 10
    try std.testing.expectEqual(@as(u32, 115), output[1]); // 110 + 5
    try std.testing.expectEqual(@as(u32, 118), output[2]); // 115 + 3
    try std.testing.expectEqual(@as(u32, 120), output[3]); // 118 + 2
}

test "svbDecodeQuad0124 with fused delta decoding" {
    // Test fused delta decoding with 0124 variant: [1, 2, 0, 4] with carry 50
    // Expected output: [51, 53, 53, 57] (prefix sums with carry)
    const control: u8 = 0b01_00_01_01; // 1 byte, 1 byte, 0 bytes, 1 byte
    const input = [_]u8{ 1, 2, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    var output: [4]u32 = undefined;

    const consumed = svbDecodeQuadWithDelta(.variant0124, control, &input, &output, 50);
    try std.testing.expectEqual(3, consumed);
    try std.testing.expectEqual(@as(u32, 51), output[0]); // 50 + 1
    try std.testing.expectEqual(@as(u32, 53), output[1]); // 51 + 2
    try std.testing.expectEqual(@as(u32, 53), output[2]); // 53 + 0
    try std.testing.expectEqual(@as(u32, 57), output[3]); // 53 + 4
}

test "svbDecodeQuadBase" {
    // Test the base function directly
    const control: u8 = 0b00_00_00_00; // All 1-byte values
    const input = [_]u8{ 1, 2, 3, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

    const result = svbDecodeQuadBase(.variant1234, control, &input);
    try std.testing.expectEqual(@as(usize, 4), result.consumed);
    try std.testing.expectEqual(@as(u32, 1), result.values[0]);
    try std.testing.expectEqual(@as(u32, 2), result.values[1]);
    try std.testing.expectEqual(@as(u32, 3), result.values[2]);
    try std.testing.expectEqual(@as(u32, 4), result.values[3]);
}

test "svbDeltaDecodeInPlace SIMD" {
    var data = [_]u32{ 10, 5, 3, 2 };
    const first_value: u32 = 100;

    svbDeltaDecodeInPlace(&data, first_value);

    try std.testing.expectEqual(@as(u32, 110), data[0]); // 100 + 10
    try std.testing.expectEqual(@as(u32, 115), data[1]); // 110 + 5
    try std.testing.expectEqual(@as(u32, 118), data[2]); // 115 + 3
    try std.testing.expectEqual(@as(u32, 120), data[3]); // 118 + 2
}

test "svbDeltaDecodeInPlace SIMD large array" {
    var data = [_]u32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
    const first_value: u32 = 0;

    svbDeltaDecodeInPlace(&data, first_value);

    // Expected: prefix sums [1, 3, 6, 10, 15, 21, 28, 36, 45, 55, 66, 78, 91, 105, 120, 136]
    const expected = [_]u32{ 1, 3, 6, 10, 15, 21, 28, 36, 45, 55, 66, 78, 91, 105, 120, 136 };
    try std.testing.expectEqualSlices(u32, &expected, &data);
}

test "svbDeltaDecodeInPlace SIMD edge cases" {
    // Test empty array
    var empty: [0]u32 = undefined;
    svbDeltaDecodeInPlace(&empty, 100);

    // Test single element
    var single = [_]u32{42};
    svbDeltaDecodeInPlace(&single, 100);
    try std.testing.expectEqual(@as(u32, 142), single[0]);

    // Test two elements
    var two = [_]u32{ 10, 20 };
    svbDeltaDecodeInPlace(&two, 100);
    try std.testing.expectEqual(@as(u32, 110), two[0]);
    try std.testing.expectEqual(@as(u32, 130), two[1]);

    // Test three elements (not SIMD aligned)
    var three = [_]u32{ 1, 2, 3 };
    svbDeltaDecodeInPlace(&three, 0);
    try std.testing.expectEqual(@as(u32, 1), three[0]);
    try std.testing.expectEqual(@as(u32, 3), three[1]);
    try std.testing.expectEqual(@as(u32, 6), three[2]);
}

test "decodeValues with unrolled loop (32+ items)" {
    // Test the new unrolled loop that processes 32 items at a time
    const n = 40; // More than 32 to trigger the unrolled loop
    var output: [40]u32 = undefined;

    // Create control bytes and data for 40 items (10 quads)
    var control_bytes: [10]u8 = undefined;
    var data_bytes: [56]u8 = undefined; // Extra space for SIMD padding
    var data_offset: usize = 0;

    // Fill with simple pattern: each value is i+1 encoded as 1 byte
    for (0..10) |i| {
        // Control byte: all 1-byte codes (01 01 01 01)
        control_bytes[i] = 0b01_01_01_01;

        // Data: 4 values per quad, each 1 byte
        data_bytes[data_offset] = @as(u8, @intCast(i * 4 + 1));
        data_bytes[data_offset + 1] = @as(u8, @intCast(i * 4 + 2));
        data_bytes[data_offset + 2] = @as(u8, @intCast(i * 4 + 3));
        data_bytes[data_offset + 3] = @as(u8, @intCast(i * 4 + 4));
        data_offset += 4;
    }

    // Zero out the remaining data bytes for SIMD padding
    @memset(data_bytes[40..56], 0);

    // Construct input buffer: control bytes followed by data bytes
    var input_buffer: [66]u8 = undefined; // 10 control + 56 data
    @memcpy(input_buffer[0..10], &control_bytes);
    @memcpy(input_buffer[10..66], &data_bytes);

    // Test decodeValues with svbDecodeQuad0124
    decodeValues(
        n,
        0,
        n,
        &input_buffer,
        &output,
        Variant.variant0124,
        .no_delta,
        {},
    );

    // Verify output values
    for (0..40) |i| {
        try std.testing.expectEqual(@as(u32, @intCast(i + 1)), output[i]);
    }
}

test "svbEncodeQuad0124" {
    // Test encoding values: [1, 2, 0, 4]
    // Expected: 1 (1 byte), 2 (1 byte), 0 (0 bytes), 4 (1 byte)
    // Control bits: 01 01 00 01 = 0b01_00_01_01 = 0x45
    const input = [4]u32{ 1, 2, 0, 4 };
    var output_data: [16]u8 = undefined;
    var control_byte: u8 = undefined;

    const bytes_written = svbEncodeQuad0124(input, &output_data, &control_byte);

    try std.testing.expectEqual(@as(usize, 3), bytes_written);
    try std.testing.expectEqual(@as(u8, 0b01_00_01_01), control_byte);
    try std.testing.expectEqual(@as(u8, 1), output_data[0]);
    try std.testing.expectEqual(@as(u8, 2), output_data[1]);
    try std.testing.expectEqual(@as(u8, 4), output_data[2]);
}

test "svbEncodeQuad0124 various sizes" {
    // Test values with different sizes: [0, 255, 65535, 0x12345678]
    // Expected: 0 (0 bytes), 255 (1 byte), 65535 (2 bytes), 0x12345678 (4 bytes)
    // Control bits: 00 01 10 11 = 0b11_10_01_00 = 0xE4
    const input = [4]u32{ 0, 255, 65535, 0x12345678 };
    var output_data: [16]u8 = undefined;
    var control_byte: u8 = undefined;

    const bytes_written = svbEncodeQuad0124(input, &output_data, &control_byte);

    try std.testing.expectEqual(@as(usize, 7), bytes_written); // 0 + 1 + 2 + 4
    try std.testing.expectEqual(@as(u8, 0b11_10_01_00), control_byte);

    // Check data bytes
    try std.testing.expectEqual(@as(u8, 255), output_data[0]);
    try std.testing.expectEqual(@as(u16, 65535), std.mem.readInt(u16, output_data[1..3], .little));
    try std.testing.expectEqual(@as(u32, 0x12345678), std.mem.readInt(u32, output_data[3..7], .little));
}

test "svbEncodeQuad1234" {
    // Test encoding values: [1, 2, 3, 4]
    // Expected: 1 (1 byte), 2 (1 byte), 3 (1 byte), 4 (1 byte)
    // Control bits: 00 00 00 00 = 0b00_00_00_00 = 0x00
    const input = [4]u32{ 1, 2, 3, 4 };
    var output_data: [16]u8 = undefined;
    var control_byte: u8 = undefined;

    const bytes_written = svbEncodeQuad1234(input, &output_data, &control_byte);

    try std.testing.expectEqual(@as(usize, 4), bytes_written);
    try std.testing.expectEqual(@as(u8, 0b00_00_00_00), control_byte);
    try std.testing.expectEqual(@as(u8, 1), output_data[0]);
    try std.testing.expectEqual(@as(u8, 2), output_data[1]);
    try std.testing.expectEqual(@as(u8, 3), output_data[2]);
    try std.testing.expectEqual(@as(u8, 4), output_data[3]);
}

test "svbEncodeQuad1234 various sizes" {
    // Test values with different sizes: [255, 65535, 0xFFFFFF, 0x12345678]
    // Expected: 255 (1 byte), 65535 (2 bytes), 0xFFFFFF (3 bytes), 0x12345678 (4 bytes)
    // Control bits: 00 01 10 11 = 0b11_10_01_00 = 0xE4
    const input = [4]u32{ 255, 65535, 0xFFFFFF, 0x12345678 };
    var output_data: [16]u8 = undefined;
    var control_byte: u8 = undefined;

    const bytes_written = svbEncodeQuad1234(input, &output_data, &control_byte);

    try std.testing.expectEqual(@as(usize, 10), bytes_written); // 1 + 2 + 3 + 4
    try std.testing.expectEqual(@as(u8, 0b11_10_01_00), control_byte);

    // Check data bytes
    try std.testing.expectEqual(@as(u8, 255), output_data[0]);
    try std.testing.expectEqual(@as(u16, 65535), std.mem.readInt(u16, output_data[1..3], .little));
    try std.testing.expectEqual(@as(u24, 0xFFFFFF), std.mem.readInt(u24, output_data[3..6], .little));
    try std.testing.expectEqual(@as(u32, 0x12345678), std.mem.readInt(u32, output_data[6..10], .little));
}

test "svbEncodeQuad1234 with zeros" {
    // Test encoding values with zeros: [0, 1, 0, 255]
    // Expected: 0 (1 byte), 1 (1 byte), 0 (1 byte), 255 (1 byte)
    // Control bits: 00 00 00 00 = 0b00_00_00_00 = 0x00
    const input = [4]u32{ 0, 1, 0, 255 };
    var output_data: [16]u8 = undefined;
    var control_byte: u8 = undefined;

    const bytes_written = svbEncodeQuad1234(input, &output_data, &control_byte);

    try std.testing.expectEqual(@as(usize, 4), bytes_written); // 1 + 1 + 1 + 1
    try std.testing.expectEqual(@as(u8, 0b00_00_00_00), control_byte);
    try std.testing.expectEqual(@as(u8, 0), output_data[0]);
    try std.testing.expectEqual(@as(u8, 1), output_data[1]);
    try std.testing.expectEqual(@as(u8, 0), output_data[2]);
    try std.testing.expectEqual(@as(u8, 255), output_data[3]);
}

test "svbEncodeQuadSize0124" {
    // Test size calculation for [0, 255, 65535, 0x12345678]
    const input = [4]u32{ 0, 255, 65535, 0x12345678 };
    const size = svbEncodeQuadSize0124(input);
    try std.testing.expectEqual(@as(usize, 7), size); // 0 + 1 + 2 + 4
}

test "svbEncodeQuadSize0124 all zeros" {
    const input = [4]u32{ 0, 0, 0, 0 };
    const size = svbEncodeQuadSize0124(input);
    try std.testing.expectEqual(@as(usize, 0), size);
}

test "svbEncodeQuadSize0124 all max values" {
    const input = [4]u32{ 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF };
    const size = svbEncodeQuadSize0124(input);
    try std.testing.expectEqual(@as(usize, 16), size); // 4 + 4 + 4 + 4
}

test "svbEncodeQuadSize1234" {
    // Test size calculation for [255, 65535, 0xFFFFFF, 0x12345678]
    const input = [4]u32{ 255, 65535, 0xFFFFFF, 0x12345678 };
    const size = svbEncodeQuadSize1234(input);
    try std.testing.expectEqual(@as(usize, 10), size); // 1 + 2 + 3 + 4
}

test "svbEncodeQuadSize1234 all small values" {
    const input = [4]u32{ 1, 2, 3, 4 };
    const size = svbEncodeQuadSize1234(input);
    try std.testing.expectEqual(@as(usize, 4), size); // 1 + 1 + 1 + 1
}

test "svbDecodeQuad0124_minus1 SIMD" {
    // Test simple case: counts [1, 2, 1, 4] encoded as [0, 1, 0, 3]
    // 0 (0 bytes), 1 (1 byte), 0 (0 bytes), 3 (1 byte)
    // Control bits: 00 01 00 01 = codes [0, 1, 0, 1]
    const control: u8 = 0b01_00_01_00;
    const input = [_]u8{ 1, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    var output: [4]u32 = undefined;

    const consumed = svbDecodeQuad(.variant0124_minus1, control, &input, &output);
    try std.testing.expectEqual(2, consumed);
    try std.testing.expectEqual(@as(u32, 1), output[0]); // 0 + 1
    try std.testing.expectEqual(@as(u32, 2), output[1]); // 1 + 1
    try std.testing.expectEqual(@as(u32, 1), output[2]); // 0 + 1
    try std.testing.expectEqual(@as(u32, 4), output[3]); // 3 + 1
}

test "svbEncodeQuadSize1234 all max values" {
    const input = [4]u32{ 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF };
    const size = svbEncodeQuadSize1234(input);
    try std.testing.expectEqual(@as(usize, 16), size); // 4 + 4 + 4 + 4
}

test "svbEncodeQuadSize1234 with zeros" {
    // Test size calculation with zeros: [0, 1, 0, 255]
    // Expected: 0 (1 byte), 1 (1 byte), 0 (1 byte), 255 (1 byte)
    const input = [4]u32{ 0, 1, 0, 255 };
    const size = svbEncodeQuadSize1234(input);
    try std.testing.expectEqual(@as(usize, 4), size); // 1 + 1 + 1 + 1
}

test "decodeValues with fused delta decoding" {
    // Test the refactored decodeValues function with fused delta decoding
    const n = 8; // 2 quads
    var output: [8]u32 = undefined;

    // Create control bytes and data for 8 items (2 quads)
    var control_bytes: [2]u8 = undefined;
    var data_bytes: [24]u8 = undefined; // Extra space for SIMD padding
    var data_offset: usize = 0;

    // First quad: [10, 5, 3, 2] - all 1-byte values
    control_bytes[0] = 0b00_00_00_00; // All 1-byte codes for variant1234
    data_bytes[data_offset] = 10;
    data_bytes[data_offset + 1] = 5;
    data_bytes[data_offset + 2] = 3;
    data_bytes[data_offset + 3] = 2;
    data_offset += 4;

    // Second quad: [1, 1, 1, 1] - all 1-byte values
    control_bytes[1] = 0b00_00_00_00; // All 1-byte codes for variant1234
    data_bytes[data_offset] = 1;
    data_bytes[data_offset + 1] = 1;
    data_bytes[data_offset + 2] = 1;
    data_bytes[data_offset + 3] = 1;
    data_offset += 4;

    // Zero out the remaining data bytes for SIMD padding
    @memset(data_bytes[8..24], 0);

    // Construct input buffer: control bytes followed by data bytes
    var input_buffer: [26]u8 = undefined; // 2 control + 24 data
    @memcpy(input_buffer[0..2], &control_bytes);
    @memcpy(input_buffer[2..26], &data_bytes);

    // Test decodeValues with fused delta decoding
    decodeValues(
        n,
        0,
        n,
        &input_buffer,
        &output,
        Variant.variant1234,
        .delta,
        100, // first_value
    );

    // Verify output values with delta decoding applied
    // First quad: [100+10, 110+5, 115+3, 118+2] = [110, 115, 118, 120]
    // Second quad: [120+1, 121+1, 122+1, 123+1] = [121, 122, 123, 124]
    try std.testing.expectEqual(@as(u32, 110), output[0]);
    try std.testing.expectEqual(@as(u32, 115), output[1]);
    try std.testing.expectEqual(@as(u32, 118), output[2]);
    try std.testing.expectEqual(@as(u32, 120), output[3]);
    try std.testing.expectEqual(@as(u32, 121), output[4]);
    try std.testing.expectEqual(@as(u32, 122), output[5]);
    try std.testing.expectEqual(@as(u32, 123), output[6]);
    try std.testing.expectEqual(@as(u32, 124), output[7]);
}
