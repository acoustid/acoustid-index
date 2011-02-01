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

#include "store/output_stream.h"
#include "segment_index_writer.h"

SegmentIndexWriter::SegmentIndexWriter(OutputStream *output)
	: m_output(output), m_blockSize(0), m_indexInterval(0),
	  m_headerWritten(false), m_keyCount(0), m_lastKey(0),
	  m_keyCountPosition(0)
{
}

SegmentIndexWriter::~SegmentIndexWriter()
{
	close();
}

void SegmentIndexWriter::maybeWriteHeader()
{
	if (!m_headerWritten) {
		m_output->writeInt32(m_blockSize);
		m_output->writeInt32(m_indexInterval);
		m_keyCountPosition = m_output->position();
		m_output->writeInt32(0);
		m_headerWritten = true;
	}
}

void SegmentIndexWriter::addItem(uint32_t key)
{
	maybeWriteHeader();
	m_output->writeVInt32(key - m_lastKey);
	m_lastKey = key;
	m_keyCount++;
}

void SegmentIndexWriter::close()
{
	maybeWriteHeader();
	m_output->seek(m_keyCountPosition);
	m_output->writeInt32(m_keyCount);
	m_output->flush();
}

