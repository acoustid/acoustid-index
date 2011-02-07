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

#ifndef ACOUSTID_SEGMENT_INFO_H_
#define ACOUSTID_SEGMENT_INFO_H_

#include "common.h"

class SegmentInfo
{
public:
	SegmentInfo(const QString &name = QString(), size_t numDocs = 0)
		: m_name(name), m_numDocs(numDocs)
	{
	}

	QString name() const
	{
		return m_name;
	}

	void setName(const QString &name)
	{
		m_name = name;
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
	QString m_name;
	size_t m_blockSize;
	size_t m_numDocs;
};

#endif
