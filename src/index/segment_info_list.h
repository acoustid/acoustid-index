// Acoustid Index -- Inverted index for audio fingerprints
// Copyright (C) 2011  Lukas Lalinsky
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

#ifndef ACOUSTID_SEGMENT_INFO_LIST_H_
#define ACOUSTID_SEGMENT_INFO_LIST_H_

#include <QList>
#include <QStringList>
#include "common.h"
#include "segment_info.h"

class Directory;
class InputStream;
class OutputStream;

class SegmentInfoList
{
public:
	SegmentInfoList()
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

	void clear();
	void add(const SegmentInfo &info);

	void read(InputStream *input);
	void write(OutputStream *output);

	static int findCurrentRevision(Directory *dir);
	static QString segmentsFileName(int revision);
	static int segmentsRevision(const QString &fileName);

private:
	QList<SegmentInfo> m_infos;
};

#endif

