// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SEGMENT_INFO_LIST_H_
#define ACOUSTID_SEGMENT_INFO_LIST_H_

#include <QList>
#include <QStringList>
#include <algorithm>
#include "common.h"
#include "segment_info.h"

namespace Acoustid {

class Directory;
class InputStream;
class OutputStream;

class SegmentInfoList
{
public:
	typedef QList<SegmentInfo>::const_iterator const_iterator;
	typedef QList<SegmentInfo>::iterator iterator;

	SegmentInfoList() : m_nextSegmentNum(0)
	{
	}

	SegmentInfoList(const SegmentInfoList &other)
		: m_nextSegmentNum(other.lastSegmetNum()),
		  m_infos(other.infos())
	{
	}

	iterator begin()
	{
		return m_infos.begin();
	}

	const_iterator begin() const
	{
		return m_infos.begin();
	}

	iterator end()
	{
		return m_infos.end();
	}

	const_iterator end() const
	{
		return m_infos.end();
	}

	size_t size() const 
	{
		return segmentCount();
	}

	size_t segmentCount() const
	{
		return m_infos.size();
	}

	const SegmentInfo &info(size_t i) const
	{
		return m_infos[i];
	}

	const QList<SegmentInfo> &infos() const
	{
		return m_infos;
	}

	size_t lastSegmetNum() const
	{
		return m_nextSegmentNum;
	}

	size_t incNextSegmentId()
	{
		return m_nextSegmentNum++;
	}

	void setNextSegmentId(size_t n)
	{
		m_nextSegmentNum = n;
	}

	void clear();
	void add(const SegmentInfo &info);

	void read(InputStream *input);
	void write(OutputStream *output);

	static int findCurrentRevision(Directory *dir);
	static QString segmentsFileName(int revision);
	static int segmentsRevision(const QString &fileName);

private:
	QList<SegmentInfo> m_infos;
	size_t m_nextSegmentNum;
};

}

#endif

