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

#ifndef ACOUSTID_INDEX_SEGMENT_INDEX_WRITER_H_
#define ACOUSTID_INDEX_SEGMENT_INDEX_WRITER_H_

#include <QList>
#include "common.h"

namespace Acoustid {

class OutputStream;

class SegmentIndexWriter
{
public:
	SegmentIndexWriter(OutputStream *output);
	virtual ~SegmentIndexWriter();

	size_t blockSize() const { return m_blockSize; }
	void setBlockSize(size_t i) { m_blockSize = i; }

	void addItem(uint32_t key);
	void close();

private:
	void maybeWriteHeader();

	size_t m_blockSize;
	uint32_t m_lastKey;
	size_t m_keyCount;
	size_t m_keyCountPosition;
	bool m_headerWritten;
	OutputStream *m_output;
};

}

#endif
