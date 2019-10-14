// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_INDEX_READER_H_
#define ACOUSTID_INDEX_SEGMENT_INDEX_READER_H_

#include "common.h"
#include "segment_index.h"

namespace Acoustid {

class InputStream;

class SegmentIndexReader
{
public:
	SegmentIndexReader(InputStream *input, size_t blockCount);
	virtual ~SegmentIndexReader();

	SegmentIndexSharedPtr read();

private:
	ScopedPtr<InputStream> m_input;
	size_t m_blockCount;
};

}

#endif
