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
#include "util/search_utils.h"
#include "segment_index.h"

using namespace Acoustid;

SegmentIndex::SegmentIndex(size_t blockSize, size_t indexInterval, size_t keyCount)
	: m_blockSize(blockSize), m_indexInterval(indexInterval)
{
	// calculate the number of levels
	m_levelCount = 1;
	size_t levelKeyCount = keyCount;
	while (levelKeyCount > indexInterval) {
		levelKeyCount = (levelKeyCount + indexInterval - 1) / indexInterval;
		m_levelCount++;
	}
	// allocate enough memory
	m_levelKeyCounts.reset(new size_t[m_levelCount]);
	m_levelKeys.reset(new uint32_t*[m_levelCount]);
	levelKeyCount = keyCount;
	for (size_t i = 0; i < m_levelCount; i++) {
		m_levelKeyCounts[i] = levelKeyCount;
		m_levelKeys[i] = new uint32_t[levelKeyCount];
		levelKeyCount = (levelKeyCount + indexInterval - 1) / indexInterval;
	}
}

SegmentIndex::~SegmentIndex()
{
	for (size_t i = 0; i < m_levelCount; i++) {
		delete[] m_levelKeys[i];
	}
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

bool SegmentIndex::search(uint32_t key, size_t *firstBlock, size_t *lastBlock)
{
	ssize_t pos = searchFirstSmaller(m_levelKeys[0], 0, m_levelKeyCounts[0], key);
	if (pos == -1) {
		if (m_levelKeys[0][0] > key) {
			return false;
		}
		pos = 0;
	}
	*firstBlock = pos;
	*lastBlock = scanFirstGreater(m_levelKeys[0], *firstBlock, m_levelKeyCounts[0], key) - 1;
	return true;
/*
	size_t level = m_levelCount - 1;
	size_t lo = 0;
	do {
		uint32_t *keys = m_levelKeys[level];
		ssize_t pos = searchFirstSmaller(keys, lo, std::min(lo + m_indexInterval, m_levelKeyCounts[level]), key);
		if (pos == -1) {
			if (keys[0] > key) {
				return false;
			}
			pos = 0;
		}
		lo = m_indexInterval * pos;
	} while (level--);
	*firstBlock = lo / m_indexInterval;
	*lastBlock = scanFirstGreater(m_levelKeys[0], *firstBlock, m_levelKeyCounts[0], key) - 1;
	return true;*/
}

/*class MultiBinarySearcher
{

	void search(size_t left, size_t right, size_t highKey, size_t lowKey);

private:
	uint32_t *m_data;
	uint32_t *m_keys;
};

void MultiBinarySearcher::search(size_t lo, size_t hi, size_t loKey, size_t hiKey)
{
	if (loKey >= hiKey) {
		return;
	}
	size_t midKey = loKey + (hiKey - loKey) / 2;

	size_t first = searchFirstSmaller(lo, hi, m_keys[midKey]);
	if (first == -1) {
		for (int i = 0; i <= midKey; i++) {
			m_min[i] = -1;
		}
		search(lo, hi, midKey + 1, hiKey);
		return;
	}

	size_t last = scanFirstGreater(first, hi, m_keys[midKey]);
	m_min[midKey] = first;
	m_max[midKey] = last;

	search(left, position + 1, lowKey, middleKey);
	search(position, right, middleKey + 1, highKey);
}
*/
