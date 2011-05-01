// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SEGMENT_INFO_LIST_H_
#define ACOUSTID_SEGMENT_INFO_LIST_H_

#include <QList>
#include <QStringList>
#include "common.h"
#include "segment_info.h"

namespace Acoustid {

class Directory;
class InputStream;
class OutputStream;

class SegmentInfoList
{
public:
	SegmentInfoList() : m_nextSegmentNum(0)
	{
	}

	size_t segmentCount() const
	{
		return m_infos.size();
	}

	const SegmentInfo &info(size_t i) const
	{
		return m_infos[i];
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

