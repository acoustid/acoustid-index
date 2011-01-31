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

#include <math.h>
#include "store/output_stream.h"
#include "segment_index.h"

SegmentIndex::SegmentIndex(size_t blockSize, size_t indexInterval, size_t keyCount)
	: m_blockSize(blockSize), m_indexInterval(indexInterval)
{
	// calculate the number of levels
	m_levelCount = 1;
	size_t levelKeyCount = keyCount;
	while (levelKeyCount > indexInterval) {
		levelKeyCount /= indexInterval;
		m_levelCount++;
	}
	// allocate enough memory
	m_levelKeyCounts = new size_t[m_levelCount];
	m_levelKeys = new uint32_t*[m_levelCount];
	levelKeyCount = keyCount;
	for (size_t i = 0; i < m_levelCount; i++) {
		m_levelKeyCounts[i] = levelKeyCount;
		m_levelKeys[i] = new uint32_t[levelKeyCount];
		levelKeyCount /= indexInterval;
	}
}

SegmentIndex::~SegmentIndex()
{
	for (size_t i = 0; i < m_levelCount; i++) {
		delete[] m_levelKeys[i];
	}
	delete[] m_levelKeys;
	delete[] m_levelKeyCounts;
}

void SegmentIndex::rebuild()
{
	int skipInterval = m_indexInterval;
	for (size_t level = 1; level < m_levelCount; level++) {
		uint32_t *dest = &m_levelKeys[level][0];
		for (size_t i = 0; i < m_levelKeyCounts[0]; i += skipInterval) {
			*dest++ = m_levelKeys[0][i];
		}
		skipInterval *= m_indexInterval;
	}
}

