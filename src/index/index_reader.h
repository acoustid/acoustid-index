// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_READER_H_
#define ACOUSTID_INDEX_READER_H_

#include "common.h"
#include "segment_info_list.h"

namespace Acoustid {

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

protected:
	void setRevision(int revision)
	{
		m_revision = revision;
	}

	void setSegmentInfos(const SegmentInfoList &infos)
	{
		m_infos = infos;
	}

	Directory *m_dir;
	int m_revision;
	SegmentInfoList m_infos;
};

}

#endif
