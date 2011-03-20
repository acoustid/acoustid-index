// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_INDEX_READER_H_
#define ACOUSTID_INDEX_SEGMENT_INDEX_READER_H_

#include "common.h"

namespace Acoustid {

class InputStream;
class SegmentIndex;

class SegmentIndexReader
{
public:
	SegmentIndexReader(InputStream *input);
	virtual ~SegmentIndexReader();

	SegmentIndex *read();

private:
	InputStream *m_input;
};

}

#endif
