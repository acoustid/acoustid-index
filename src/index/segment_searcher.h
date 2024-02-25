// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_SEARCHER_H_
#define ACOUSTID_INDEX_SEGMENT_SEARCHER_H_

#include "common.h"
#include "segment_index.h"

namespace Acoustid {

class SegmentDataReader;

class SegmentSearcher
{
public:
	SegmentSearcher(SegmentIndexSharedPtr index, std::unique_ptr<SegmentDataReader> &&dataReader, uint32_t lastKey = UINT32_MAX);
	virtual ~SegmentSearcher();

	void search(const std::vector<uint32_t> &hashes, std::unordered_map<uint32_t, int> &hits);

private:
	SegmentIndexSharedPtr m_index;
	std::unique_ptr<SegmentDataReader> m_dataReader;
	uint32_t m_lastKey;
};

}

#endif
