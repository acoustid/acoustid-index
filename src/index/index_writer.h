// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_WRITER_H_
#define ACOUSTID_INDEX_WRITER_H_

#include "common.h"
#include "index_info.h"
#include "segment_merge_policy.h"
#include "index_reader.h"

namespace Acoustid {

class SegmentDataWriter;

class IndexWriter : public IndexReader
{
public:
	IndexWriter(Directory *dir);
	virtual ~IndexWriter();

	size_t maxSegmentBufferSize() const
	{
		return m_maxSegmentBufferSize;
	}

	void setMaxSegmentBufferSize(size_t maxSegmentBufferSize)
	{
		m_maxSegmentBufferSize = maxSegmentBufferSize;
	}

	void open(bool create = false);

	SegmentMergePolicy *segmentMergePolicy()
	{
		return m_mergePolicy;
	}

	void addDocument(uint32_t id, uint32_t *terms, size_t length);
	void commit();

private:

	void flush();
	void maybeFlush();
	void maybeMerge(IndexInfo* info);

	SegmentDataWriter *segmentDataWriter(const SegmentInfo &info);

	size_t m_maxSegmentBufferSize;
	std::vector<uint64_t> m_segmentBuffer;
	SegmentMergePolicy *m_mergePolicy;
};

}

#endif
