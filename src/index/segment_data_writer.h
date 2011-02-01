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

#ifndef ACOUSTID_INDEX_SEGMENT_DATA_WRITER_H_
#define ACOUSTID_INDEX_SEGMENT_DATA_WRITER_H_

#include "common.h"

class OutputStream;
class SegmentIndexWriter;

typedef QPair<uint32_t, uint32_t> Int32Pair;

class SegmentDataWriter
{
public:
	SegmentDataWriter(OutputStream *output, SegmentIndexWriter *indexWriter, size_t blockSize);
	virtual ~SegmentDataWriter();

	size_t blockSize() { return m_blockSize; }
	void setBlockSize(size_t blockSize);

	void addItem(uint32_t key, uint32_t value);
	void close();

private:
	void writeBlock();

	OutputStream *m_output;
	SegmentIndexWriter *m_indexWriter;
	size_t m_blockSize;
	uint32_t m_lastKey;
	uint32_t m_lastValue;
	size_t m_itemCount;
	uint8_t *m_ptr;
	uint8_t *m_buffer;
};

#endif
