// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "store/input_stream.h"
#include "segment_index.h"
#include "segment_index_reader.h"

using namespace Acoustid;

SegmentIndexReader::SegmentIndexReader(InputStream *input)
	: m_input(input)
{
}

SegmentIndexReader::~SegmentIndexReader()
{
	delete m_input;
}

SegmentIndex *SegmentIndexReader::read()
{
	size_t blockSize = m_input->readInt32();
	size_t keyCount = m_input->readInt32();
	SegmentIndex *index = new SegmentIndex(blockSize, keyCount);
	uint32_t *keys = index->levelKeys(0), lastKey = 0;
	for (size_t i = 0; i < keyCount; i++) {
		lastKey += m_input->readVInt32();
		*keys++ = lastKey;
	}
	index->rebuild();
	return index;
}

