// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SEGMENT_INFO_H_
#define ACOUSTID_SEGMENT_INFO_H_

#include "common.h"

namespace Acoustid {

class SegmentInfo
{
public:
	SegmentInfo(int id = 0, size_t numDocs = 0)
		: m_id(id), m_numDocs(numDocs)
	{
	}

	QString name() const
	{
		return QString("segment_%1").arg(m_id);
	}

	QString indexFileName() const
	{
		return name() + ".fii";
	}

	QString dataFileName() const
	{
		return name() + ".fid";
	}

	void setId(int id)
	{
		m_id = id;
	}

	int id() const
	{
		return m_id;
	}

	size_t numDocs() const
	{
		return m_numDocs;
	}

	void setNumDocs(size_t numDocs)
	{
		m_numDocs = numDocs;
	}

private:
	int m_id;
	size_t m_numDocs;
};

}

#endif
