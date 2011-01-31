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
	: m_output(output), m_blockSize(0), m_indexInterval(0)
{
}

SegmentIndexWriter::~SegmentIndexWriter()
{
}

void SegmentIndexWriter::addItem(uint32_t key)
{
	m_keys.append(key);
}

void SegmentIndexWriter::close()
{
	m_output->writeInt32(m_blockSize);
	m_output->writeInt32(m_indexInterval);
	size_t keyCount = m_keys.size();
	int skipInterval = 1;
	while (keyCount >= m_indexInterval) {
		m_output->writeInt32(keyCount);
		for (size_t i = 0; i < m_keys.size(); i += skipInterval) {
			m_output->writeInt32(m_keys.at(i));
		}
		keyCount /= m_indexInterval;
		skipInterval *= m_indexInterval;
	}
	m_output->flush();
}
