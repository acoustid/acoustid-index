// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_SEARCHER_H_
#define ACOUSTID_INDEX_SEGMENT_SEARCHER_H_

#include "common.h"
#include "segment_index.h"

namespace Acoustid {

class SegmentDataReader;
class Collector;

class SegmentSearcher
{
public:
	SegmentSearcher(SegmentIndexSharedPtr index, SegmentDataReader *dataReader);
	virtual ~SegmentSearcher();

	/**
	 * Search for the fingerprint in one segment.
	 *
	 * The fingerprint must be sorted.
	 */
	void search(uint32_t *fingerprint, size_t length, Collector *collector);

private:
	SegmentIndexSharedPtr m_index;
	SegmentDataReader *m_dataReader;
};

}

#endif
