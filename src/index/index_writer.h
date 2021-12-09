// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_WRITER_H_
#define ACOUSTID_INDEX_WRITER_H_

#include "common.h"
#include "index_info.h"
#include "segment_merge_policy.h"
#include "index_reader.h"

namespace Acoustid {

class Index;
class SegmentDataWriter;

class IndexWriter : public IndexReader
{
public:
	IndexWriter(DirectorySharedPtr dir, const IndexInfo& info);
	IndexWriter(IndexSharedPtr index, bool alreadyHasLock = false);
	virtual ~IndexWriter();

	size_t maxSegmentBufferSize() const
	{
		return m_maxSegmentBufferSize;
	}

	void setMaxSegmentBufferSize(size_t maxSegmentBufferSize)
	{
		m_maxSegmentBufferSize = maxSegmentBufferSize;
	}

	SegmentMergePolicy* segmentMergePolicy()
	{
		return m_mergePolicy.get();
	}

	void addDocument(uint32_t id, const uint32_t *terms, size_t length);
	void setAttribute(const QString &name, const QString &value);
	void commit();
	void cleanup();
	void optimize();

private:
	void flush();
	void maybeFlush();
	void maybeMerge();
	void merge(const QList<int>& merge);

	SegmentDataWriter *segmentDataWriter(const SegmentInfo& info);

	uint32_t m_maxDocumentId;
	size_t m_maxSegmentBufferSize;
	std::vector<uint64_t> m_segmentBuffer;
	std::unique_ptr<SegmentMergePolicy> m_mergePolicy;
};

}

#endif
