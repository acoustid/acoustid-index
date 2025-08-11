const std = @import("std");

// Length tables - how many bytes are consumed for each control byte value
const LENGTH_TABLE_0124 = blk: {
    var table: [256]u8 = undefined;
    for (0..256) |control| {
        var len: u8 = 0;
        for (0..4) |i| {
            const code = @as(u2, @intCast((control >> @intCast(2 * i)) & 0x3));
            len += switch (code) {
                0 => 0, // 0 bytes for zero
                1 => 1, // 1 byte
                2 => 2, // 2 bytes
                3 => 4, // 4 bytes
            };
        }
        table[control] = len;
    }
    break :blk table;
};

const LENGTH_TABLE_1234 = blk: {
    var table: [256]u8 = undefined;
    for (0..256) |control| {
        var len: u8 = 0;
        for (0..4) |i| {
            const code = @as(u2, @intCast((control >> @intCast(2 * i)) & 0x3));
            len += switch (code) {
                0 => 1, // 1 byte
                1 => 2, // 2 bytes
                2 => 3, // 3 bytes
                3 => 4, // 4 bytes
            };
        }
        table[control] = len;
    }
    break :blk table;
};

// Shuffle masks for vector implementation - computed at comptime
const SHUFFLE_TABLE_0124 = blk: {
    @setEvalBranchQuota(10000); // Increase compile time evaluation limit
    var table: [256][16]i8 = undefined;
    for (0..256) |control| {
        var mask: [16]i8 = [_]i8{-1} ** 16;
        var in_pos: u8 = 0;
        var out_pos: u8 = 0;
        
        for (0..4) |i| {
            const code = @as(u2, @intCast((control >> @intCast(2 * i)) & 0x3));
            const byte_count = switch (code) {
                0 => 0, // 0 bytes for zero - output stays -1 (zero)
                1 => 1, // 1 byte
                2 => 2, // 2 bytes
                3 => 4, // 4 bytes
            };
            
            // Set mask for this 32-bit output
            for (0..byte_count) |j| {
                mask[out_pos + j] = @intCast(in_pos + j);
            }
            // Remaining bytes in this 32-bit slot stay -1 (zero)
            
            in_pos += byte_count;
            out_pos += 4; // Each output is 4 bytes (u32)
        }
        table[control] = mask;
    }
    break :blk table;
};

const SHUFFLE_TABLE_1234 = blk: {
    @setEvalBranchQuota(10000); // Increase compile time evaluation limit
    var table: [256][16]i8 = undefined;
    for (0..256) |control| {
        var mask: [16]i8 = [_]i8{-1} ** 16;
        var in_pos: u8 = 0;
        var out_pos: u8 = 0;
        
        for (0..4) |i| {
            const code = @as(u2, @intCast((control >> @intCast(2 * i)) & 0x3));
            const byte_count = switch (code) {
                0 => 1, // 1 byte
                1 => 2, // 2 bytes
                2 => 3, // 3 bytes
                3 => 4, // 4 bytes
            };
            
            // Set mask for this 32-bit output
            for (0..byte_count) |j| {
                mask[out_pos + j] = @intCast(in_pos + j);
            }
            // Remaining bytes in this 32-bit slot stay -1 (zero)
            
            in_pos += byte_count;
            out_pos += 4; // Each output is 4 bytes (u32)
        }
        table[control] = mask;
    }
    break :blk table;
};


// Vector implementation for svb_decode_quad_0124
pub fn svbDecodeQuad0124(control: u8, in_data: []const u8, out: *[4]u32) usize {
    std.debug.assert(in_data.len >= 16); // Need padding for safe vector load
    
    // Load 16 bytes of input data as a vector
    const data_vec: @Vector(16, u8) = in_data[0..16].*;
    
    // Get shuffle mask for this control byte
    const shuffle_mask = SHUFFLE_TABLE_0124[control];
    
    // Apply shuffle to rearrange bytes - Zig doesn't have direct shuffle for mixed types,
    // so we'll use the scalar version for now but with vector-friendly data layout
    const result_vec = @shuffle(u8, data_vec, undefined, shuffle_mask);
    
    // Convert result to u32 vector directly
    const result_u32: @Vector(4, u32) = @bitCast(result_vec);
    out.* = result_u32;
    
    return LENGTH_TABLE_0124[control];
}


// Vector implementation for svb_decode_quad_1234
pub fn svbDecodeQuad1234(control: u8, in_data: []const u8, out: *[4]u32) usize {
    std.debug.assert(in_data.len >= 16); // Need padding for safe vector load
    
    // Load 16 bytes of input data as a vector
    const data_vec: @Vector(16, u8) = in_data[0..16].*;
    
    // Get shuffle mask for this control byte
    const shuffle_mask = SHUFFLE_TABLE_1234[control];
    
    // Apply shuffle to rearrange bytes
    const result_vec = @shuffle(u8, data_vec, undefined, shuffle_mask);
    
    // Convert result to u32 vector directly
    const result_u32: @Vector(4, u32) = @bitCast(result_vec);
    out.* = result_u32;
    
    return LENGTH_TABLE_1234[control];
}


// In-place delta decoding - vector implementation
pub fn svbDeltaDecodeInPlace(data: []u32, first_value: u32) void {
    if (data.len == 0) return;
    
    data[0] += first_value;
    if (data.len == 1) return;
    
    var carry = data[0];
    var i: usize = 1;
    
    // Process 4 elements at a time with vectors
    while (i + 3 < data.len) {
        // Load 4 values
        var vec: @Vector(4, u32) = data[i..i + 4].*;
        
        // Compute prefix sum within the vector: [a, b, c, d] -> [a, a+b, a+b+c, a+b+c+d]
        // Step 1: [a, b, c, d] + [0, a, b, c] = [a, a+b, b+c, c+d]
        const shifted1 = @shuffle(u32, vec, @Vector(4, u32){ 0, 0, 0, 0 }, @Vector(4, i32){ -1, 0, 1, 2 });
        vec += shifted1;
        
        // Step 2: [a, a+b, b+c, c+d] + [0, 0, a, a+b] = [a, a+b, a+b+c, a+b+c+d]
        const shifted2 = @shuffle(u32, vec, @Vector(4, u32){ 0, 0, 0, 0 }, @Vector(4, i32){ -1, -1, 0, 1 });
        vec += shifted2;
        
        // Add carry to all elements
        const carry_vec = @as(@Vector(4, u32), @splat(carry));
        vec += carry_vec;
        
        // Store result
        data[i..i + 4].* = vec;
        
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


// Tests
test "svbDecodeQuad0124 basic" {
    const testing = std.testing;
    
    // Test control byte 0x05 = 0001 0001 = [1 byte, 0 bytes, 1 byte, 0 bytes]
    const control: u8 = 0x05;
    const input = [_]u8{ 42, 100 } ++ ([_]u8{0} ** 14); // Padded to 16 bytes
    var output: [4]u32 = undefined;
    
    const consumed = svbDecodeQuad0124(control, &input, &output);
    
    try testing.expectEqual(@as(usize, 2), consumed);
    try testing.expectEqual(@as(u32, 42), output[0]);
    try testing.expectEqual(@as(u32, 0), output[1]);
    try testing.expectEqual(@as(u32, 100), output[2]);
    try testing.expectEqual(@as(u32, 0), output[3]);
}

test "svbDecodeQuad1234 basic" {
    const testing = std.testing;
    
    // Test control byte 0x04 = 0001 0000 = [1 byte, 2 bytes, 1 byte, 1 byte]
    const control: u8 = 0x04;
    const input = [_]u8{ 42, 100, 200, 150, 250 } ++ ([_]u8{0} ** 11); // Padded to 16 bytes
    var output: [4]u32 = undefined;
    
    const consumed = svbDecodeQuad1234(control, &input, &output);
    
    try testing.expectEqual(@as(usize, 5), consumed);
    try testing.expectEqual(@as(u32, 42), output[0]);
    try testing.expectEqual(@as(u32, 200 + (100 << 8)), output[1]); // little endian
    try testing.expectEqual(@as(u32, 150), output[2]);
    try testing.expectEqual(@as(u32, 250), output[3]);
}

test "svbDeltaDecodeInPlace" {
    const testing = std.testing;
    
    var data = [_]u32{ 10, 5, 15, 3, 7, 2 };
    svbDeltaDecodeInPlace(&data, 100);
    
    // Expected: [110, 115, 130, 133, 140, 142]
    try testing.expectEqual(@as(u32, 110), data[0]); // 10 + 100
    try testing.expectEqual(@as(u32, 115), data[1]); // 5 + 110
    try testing.expectEqual(@as(u32, 130), data[2]); // 15 + 115
    try testing.expectEqual(@as(u32, 133), data[3]); // 3 + 130
    try testing.expectEqual(@as(u32, 140), data[4]); // 7 + 133
    try testing.expectEqual(@as(u32, 142), data[5]); // 2 + 140
}