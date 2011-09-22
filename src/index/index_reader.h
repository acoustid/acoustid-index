// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_READER_H_
#define ACOUSTID_INDEX_READER_H_

#include "common.h"
#include "segment_index.h"
#include "index_info.h"

namespace Acoustid {

class Index;
class SegmentIndex;
class SegmentDataReader;
class Collector;

class IndexReader
{
public:
	IndexReader(DirectorySharedPtr dir, const IndexInfo& info, const SegmentIndexMap& indexes, Index* index = NULL);
	virtual ~IndexReader();

	const IndexInfo& info() const { return m_info; }

	Index* index()
	{
		return m_index;
	}

	void setIndex(Index* index)
	{
		m_index = index;
	}

	void search(uint32_t *fingerprint, size_t length, Collector *collector);

	SegmentIndexSharedPtr segmentIndex(const SegmentInfo& segment);
	SegmentDataReader* segmentDataReader(const SegmentInfo& segment);

protected:
	DirectorySharedPtr m_dir;
	IndexInfo m_info;
	SegmentIndexMap m_indexes;
	Index* m_index;
};

}

#endif
