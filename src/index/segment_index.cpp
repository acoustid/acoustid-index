// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "segment_index.h"

#include <math.h>

#include "store/output_stream.h"
#include "util/search_utils.h"

using namespace Acoustid;

SegmentIndex::SegmentIndex(size_t blockCount) : m_blockCount(blockCount), m_keys(new uint32_t[blockCount]) {}

SegmentIndex::~SegmentIndex() {}

bool SegmentIndex::search(uint32_t key, size_t *firstBlock, size_t *lastBlock) {
    ssize_t pos = searchFirstSmaller(m_keys.get(), 0, m_blockCount, key);
    if (pos == -1) {
        if (m_keys[0] > key) {
            return false;
        }
        pos = 0;
    }
    *firstBlock = pos;
    *lastBlock = scanFirstGreater(m_keys.get(), *firstBlock, m_blockCount, key) - 1;
    return true;
}
