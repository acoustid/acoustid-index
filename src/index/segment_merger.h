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

#ifndef ACOUSTID_INDEX_SEGMENT_MERGER_H_
#define ACOUSTID_INDEX_SEGMENT_MERGER_H_

#include "common.h"
#include "segment_enum.h"
#include "segment_data_writer.h"

namespace Acoustid {

class SegmentMerger
{
public:
	SegmentMerger(SegmentDataWriter *target);

	void addSource(SegmentEnum *reader)
	{
		m_readers.append(reader);
	}

	void merge();

private:
	QList<SegmentEnum *> m_readers;
	SegmentDataWriter *m_writer;
};

}

#endif
