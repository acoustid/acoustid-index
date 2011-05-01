// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_READER_H_
#define ACOUSTID_INDEX_READER_H_

#include "common.h"
#include "collector.h"
#include "segment_index.h"
#include "segment_info_list.h"

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
		return m_revision;
	}

	Directory *directory()
	{
		return m_dir;
	}

	const SegmentInfoList &segmentInfos()
	{
		return m_infos;
	}

	void search(uint32_t *fingerprint, size_t length, Collector *collector);

protected:
	void setRevision(int revision)
	{
		m_revision = revision;
	}

	void setSegmentInfos(const SegmentInfoList &infos)
	{
		m_infos = infos;
	}

	SegmentIndexSharedPtr segmentIndex(int i);
	void closeSegmentIndex(int i);
	SegmentDataReader *segmentDataReader(int i);

	Directory *m_dir;
	QHash<int, SegmentIndexSharedPtr> m_indexes;
	int m_revision;
	SegmentInfoList m_infos;
};

}

#endif
