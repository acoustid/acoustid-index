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

#ifndef ACOUSTID_INDEX_SEGMENT_INDEX_H_
#define ACOUSTID_INDEX_SEGMENT_INDEX_H_

#include "common.h"

namespace Acoustid {

class SegmentIndex
{
public:
	SegmentIndex(size_t blockSize, size_t indexInterval, size_t keyCount);
	virtual ~SegmentIndex();

	size_t blockSize() { return m_blockSize; }
	size_t indexInterval() { return m_indexInterval; }

	size_t levelCount() { return m_levelCount; }
	size_t levelKeyCount(size_t level) { return m_levelKeyCounts[level]; }
	uint32_t *levelKeys(size_t level) { return m_levelKeys[level]; }

	uint32_t levelKey(size_t block, size_t level = 0)
	{
		return m_levelKeys[level][block];
	}

	void rebuild();

	bool search(uint32_t key, size_t *firstBlock, size_t *lastBlock);


private:
	size_t m_blockSize;
	size_t m_indexInterval;
	size_t m_levelCount;
	ScopedArrayPtr<size_t> m_levelKeyCounts;
	ScopedArrayPtr<uint32_t*> m_levelKeys;
};

}

#endif
