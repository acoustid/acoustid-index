// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_READER_H_
#define ACOUSTID_INDEX_READER_H_

#include "common.h"
#include "collector.h"
#include "segment_index.h"
#include "index_info.h"

namespace Acoustid {

class SegmentIndex;
class SegmentDataReader;

class IndexReader
{
public:
	IndexReader(Directory *dir);
	virtual ~IndexReader();

	void open();

	int revision()
	{
		return m_infos.revision();
	}

	Directory *directory()
	{
		return m_dir;
	}

	const IndexInfo &segmentInfos()
	{
		return m_infos;
	}

	void search(uint32_t *fingerprint, size_t length, Collector *collector);

protected:
	void setRevision(int revision)
	{
		m_infos.setRevision(revision);
	}

	void setSegmentInfos(const IndexInfo &infos)
	{
		m_infos = infos;
	}

	SegmentIndexSharedPtr segmentIndex(int i);
	void closeSegmentIndex(int i);
	SegmentDataReader *segmentDataReader(int i);

	Directory *m_dir;
	QHash<int, SegmentIndexSharedPtr> m_indexes;
	IndexInfo m_infos;
};

}

#endif
