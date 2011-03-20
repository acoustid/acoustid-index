// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_WRITER_H_
#define ACOUSTID_INDEX_WRITER_H_

#include "common.h"
#include "segment_info_list.h"

namespace Acoustid {

class IndexWriter
{
public:
	IndexWriter(Directory *dir, bool create = false);
	virtual ~IndexWriter();

	int revision();

	const SegmentInfoList &segmentInfoList()
	{
		return m_segmentInfos;
	}

	void addDocument(uint32_t id, uint32_t *terms, size_t length);
	void commit();

private:

	void flush();
	void maybeFlush();

	size_t m_maxSegmentBufferSize;
	size_t m_numDocsInBuffer;
	std::vector<uint64_t> m_segmentBuffer;
	Directory *m_dir;
	int m_revision;
	SegmentInfoList m_segmentInfos;
};

}

#endif
