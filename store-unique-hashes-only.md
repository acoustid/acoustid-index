# Store Unique Hashes Only - Design Notes

## Overview

Optimization to reduce hash decompression overhead by storing only unique hashes per block instead of all hash occurrences.

## Current vs Proposed Approach

### Current Approach
```
Block with items: [(1,100), (1,101), (1,102), (3,200), (5,300), (5,301)]
Stored as: [1,1,1,3,5,5] → delta encode → [1,0,0,2,2,0] → StreamVByte compress
Search: decompress all hashes → binary search → find range → decode docids for range
```

### Proposed Hybrid Approach
```
Same block items: [(1,100), (1,101), (1,102), (3,200), (5,300), (5,301)]
Stored as: 
- unique_hashes=[1,3,5] → delta encode → [1,2,2] → StreamVByte compress
- counts=[3,1,2] → StreamVByte compress (no delta encoding)
- docids compressed as before
Search: decompress unique hashes + counts → binary search → calculate offset → decode docids for range
```

## Benefits

### Performance
- **Much less decompression work**: Only unique hashes instead of all occurrences
- **Faster hash lookup**: Smaller arrays to binary search through
- **Better cache efficiency**: Smaller decompressed data
- **Reduced hot path overhead**: 60%+ less StreamVByte work for hash lookup

### Space Efficiency
- **Better compression**: Counts are small integers that compress well
- **No massive overhead**: Unlike fully uncompressed approach
- **Potential space savings**: 40-55% reduction in some cases

### Implementation
- **Consistent compression**: StreamVByte everywhere
- **Familiar patterns**: Same encode/decode logic
- **Reasonable complexity**: Single compression scheme, manageable offset calculations

## Scale Analysis (100M Fingerprints, 120 hashes each)

Total: 12 billion (hash, docid) pairs across ~24M blocks

### Realistic Scenario: 200 unique hashes per 500-item block
**Current**: 24M blocks × 525 bytes = **12.6 GB**
**Hybrid**: 24M blocks × 300 bytes = **7.2 GB**  
**Savings: -5.4 GB (-43%)**

### High Duplication: 50 unique hashes per 500-item block
**Current**: 24M blocks × 175 bytes = **4.2 GB**
**Hybrid**: 24M blocks × 80 bytes = **1.9 GB**
**Savings: -2.3 GB (-55%)**

## Block Format Changes

### New Block Header (10 bytes)
```zig
pub const BlockHeader = struct {
    first_hash: u32,         // for delta encoding unique hashes
    num_unique_hashes: u16,  // number of unique hashes in block
    counts_offset: u16,      // where compressed counts start  
    docid_list_offset: u16,  // where compressed docids start
};
```

### New Block Layout
```
[Header 10B][Compressed unique hash deltas][Compressed counts][Compressed docids]

Header layout:
bytes 0-3:  first_hash (u32)
bytes 4-5:  num_unique_hashes (u16)  
bytes 6-7:  counts_offset (u16)
bytes 8-9:  docid_list_offset (u16)
```

**Note**: No `total_items` field needed - calculated from counts array as offsets.

## Implementation Changes

### BlockReader Updates
- Add `counts` array and `counts_loaded` state
- Add `ensureCountsLoaded()` function
- Update `findHash()` to search unique hashes and calculate docid offsets
- Keep range-based docid decoding optimization

### New Decode Functions
- `decodeBlockUniqueHashes()` - decompress unique hashes with delta decoding
- `decodeBlockCounts()` - decompress counts + convert to offsets via prefix sum
- Update `decodeBlockDocids()` to work with new format

### Search Algorithm
```zig
pub fn findHash(self: *BlockReader, hash: u32) HashRange {
    self.ensureHashesLoaded();   // decompress unique hashes
    self.ensureCountsLoaded();   // decompress counts + convert to offsets
    
    // Binary search in smaller unique hashes array
    const hash_idx = std.sort.binarySearch(u32, hash, unique_hashes_slice, {}, orderU32);
    if (hash_idx == null) return {0, 0};
    
    // O(1) range calculation from offsets (no summing needed!)
    const start = if (hash_idx.? == 0) 0 else self.counts[hash_idx.? - 1];
    const end = self.counts[hash_idx.?];
    return HashRange{ .start = start, .end = end };
}
```

## Why StreamVByte for Counts + Prefix Sum Conversion

Counts are naturally small integers (typically 1-10 docids per unique hash):
- **Count 1-127**: 1 byte each in StreamVByte  
- **Count 128-16K**: 2 bytes each
- **No delta encoding needed**: Raw small integers compress excellently
- **Consistent tooling**: Same compression scheme throughout

**Prefix Sum Pattern**:
```zig
// Storage: [3, 1, 2, 4] → StreamVByte compress → store in block
// Runtime: StreamVByte decode → [3, 1, 2, 4] → prefix sum → [3, 4, 6, 10] (offsets)
```

**Benefits of Offset Conversion**:
- **O(1) total items**: `offsets[num_unique_hashes-1]` gives total
- **O(1) range calculation**: Direct offset lookup vs summing counts  
- **Consistent pattern**: Same as hash/docid delta decoding workflow

## Migration Strategy

1. **Implement new format alongside existing**
2. **Add format version to block header**
3. **Support both formats in BlockReader**
4. **Gradual migration during segment merges**
5. **Remove old format once migration complete**

## Key Implementation Details

### Decode Function Updates
```zig
fn decodeBlockCounts(header: BlockHeader, in: []const u8, out: []u32) void {
    const offset = BLOCK_HEADER_SIZE + header.counts_offset;
    streamvbyte.decodeValues(
        header.num_unique_hashes,
        0, 
        header.num_unique_hashes,
        in[offset..],
        out,
        .variant1234,  // counts are never 0
    );
    
    // Apply prefix sum to convert counts to offsets
    var sum: u32 = 0;
    for (0..header.num_unique_hashes) |i| {
        sum += out[i];
        out[i] = sum;
    }
}

fn getTotalItems(self: *BlockReader) u16 {
    self.ensureCountsLoaded(); // counts are now offsets after prefix sum
    if (self.block_header.num_unique_hashes == 0) return 0;
    return @intCast(self.counts[self.block_header.num_unique_hashes - 1]);
}
```

## Key Implementation Files

- `src/block.zig` - BlockHeader, BlockReader, decode functions
- `src/filefmt.zig` - File format version handling  
- Tests in `src/block.zig` - Update existing tests for new format

## Risk Mitigation

- **Backward compatibility**: Support both formats during transition
- **Extensive testing**: Benchmark on real fingerprint data first
- **Rollback plan**: Keep old format available if issues arise
- **Performance validation**: Measure actual decompression time reduction

## Success Metrics

- [ ] 50%+ reduction in hash decompression time
- [ ] 30%+ reduction in total block storage size
- [ ] No regression in search query latency
- [ ] Successful handling of 100M+ fingerprint scale