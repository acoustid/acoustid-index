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
	IndexReader(Directory* dir, const IndexInfo& info, const SegmentIndexMap& indexes, Index* index = NULL);
	virtual ~IndexReader();

	const IndexInfo& info() const { return m_info; }

	void search(uint32_t *fingerprint, size_t length, Collector *collector);

protected:
	SegmentIndexSharedPtr segmentIndex(const SegmentInfo& segment);
	SegmentDataReader* segmentDataReader(const SegmentInfo& segment);

	Directory* m_dir;
	IndexInfo m_info;
	SegmentIndexMap m_indexes;
	Index* m_index;
};

}

#endif
