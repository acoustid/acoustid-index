// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_SEARCHER_H_
#define ACOUSTID_INDEX_SEGMENT_SEARCHER_H_

#include "common.h"

namespace Acoustid {

class SegmentIndex;
class SegmentDataReader;
class Collector;

class SegmentSearcher
{
public:
	SegmentSearcher(SegmentIndex *index, SegmentDataReader *dataReader);
	virtual ~SegmentSearcher();

	void search(uint32_t *fingerprint, size_t length, Collector *collector);

private:
	SegmentIndex *m_index;
	SegmentDataReader *m_dataReader;
};

}

#endif
