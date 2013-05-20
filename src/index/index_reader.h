// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_READER_H_
#define ACOUSTID_INDEX_READER_H_

#include "common.h"
#include "segment_index.h"
#include "index.h"
#include "index_info.h"

namespace Acoustid {

class SegmentIndex;
class SegmentIndexDataReader;
class SegmentDocumentReader;
class Collector;

class IndexReader
{
public:
	IndexReader(DirectorySharedPtr dir, const IndexInfo& info);
	IndexReader(IndexSharedPtr index);
	virtual ~IndexReader();

	const IndexInfo& info() const { return m_info; }

	IndexSharedPtr index()
	{
		return m_index;
	}

    bool get(uint32_t id, uint32_t **fingerprint, size_t *length);

	void search(uint32_t *fingerprint, size_t length, Collector *collector);

	SegmentDocumentReader* segmentDocumentReader(const SegmentInfo& segment);
	SegmentIndexDataReader* segmentIndexDataReader(const SegmentInfo& segment);

protected:
	DirectorySharedPtr m_dir;
	IndexInfo m_info;
	IndexSharedPtr m_index;
};

}

#endif
