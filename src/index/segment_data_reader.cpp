// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "segment_data_reader.h"

#include "store/output_stream.h"

using namespace Acoustid;

SegmentDataReader::SegmentDataReader(InputStream *input, size_t blockSize) : m_input(input), m_blockSize(blockSize) {}

SegmentDataReader::~SegmentDataReader() {}

void SegmentDataReader::setBlockSize(size_t blockSize) { m_blockSize = blockSize; }

BlockDataIterator *SegmentDataReader::readBlock(size_t n, uint32_t key) {
    m_input->seek(m_blockSize * n);
    size_t length = m_input->readInt16();
    return new BlockDataIterator(m_input.get(), length, key);
}
