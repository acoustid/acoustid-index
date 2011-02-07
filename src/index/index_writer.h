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

#ifndef ACOUSTID_INDEX_WRITER_H_
#define ACOUSTID_INDEX_WRITER_H_

#include "common.h"
#include "segment_info_list.h"

class IndexWriter
{
public:
	IndexWriter(Directory *dir);
	virtual ~IndexWriter();

	void addDocument(uint32_t id, uint32_t *terms, size_t length);
	void commit();

private:

	void flush();
	void maybeFlush();

	size_t m_maxSegmentBufferSize;
	std::vector<uint64_t> m_segmentBuffer;
	/*SegmentIndexWriter *m_segmentIndexWriter;
	SegmentDataWriter *m_segmentDataWriter;*/
	Directory *m_dir;
	int m_revision;
	SegmentInfoList m_segmentInfos;
};

#endif
