// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "store/input_stream.h"
#include "segment_index.h"
#include "segment_index_reader.h"

using namespace Acoustid;

SegmentIndexReader::SegmentIndexReader(InputStream *input, size_t blockCount)
	: m_input(input), m_blockCount(blockCount)
{
}

SegmentIndexReader::~SegmentIndexReader()
{
	delete m_input;
}

SegmentIndexSharedPtr SegmentIndexReader::read()
{
	SegmentIndexSharedPtr index(new SegmentIndex(m_blockCount));
	uint32_t *keys = index->keys();
	for (size_t i = 0; i < m_blockCount; i++) {
		*keys++ = m_input->readInt32();
	}
	return index;
}

