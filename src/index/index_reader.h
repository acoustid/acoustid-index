// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_READER_H_
#define ACOUSTID_INDEX_READER_H_

#include "common.h"
#include "segment_index.h"
#include "index_info.h"

namespace Acoustid {

class SegmentIndex;
class SegmentDataReader;
class Collector;

class IndexReader
{
public:
	IndexReader(Directory* dir, const IndexInfo& info, const SegmentIndexMap& indexes);
	virtual ~IndexReader();

	void search(uint32_t *fingerprint, size_t length, Collector *collector);

protected:
	SegmentIndexSharedPtr segmentIndex(const SegmentInfo& segment);
	SegmentDataReader* segmentDataReader(const SegmentInfo& segment);

	Directory* m_dir;
	IndexInfo m_info;
	SegmentIndexMap m_indexes;
};

}

#endif
