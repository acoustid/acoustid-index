// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "store/output_stream.h"
#include "segment_index_data_reader.h"

using namespace Acoustid;

SegmentIndexDataReader::SegmentIndexDataReader(InputStream *input, size_t blockSize)
	: m_input(input), m_blockSize(blockSize)
{
}

SegmentIndexDataReader::~SegmentIndexDataReader()
{
}

void SegmentIndexDataReader::setBlockSize(size_t blockSize)
{
	m_blockSize = blockSize;
}

BlockDataIterator *SegmentIndexDataReader::readBlock(size_t n, uint32_t key)
{
	m_input->seek(m_blockSize * n);
	size_t length = m_input->readInt16();
	return new BlockDataIterator(m_input.get(), length, key);
}
