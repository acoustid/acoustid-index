// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_INDEX_H_
#define ACOUSTID_INDEX_SEGMENT_INDEX_H_

#include "common.h"

namespace Acoustid {

class SegmentIndex
{
public:
	SegmentIndex(size_t blockSize, size_t keyCount);
	virtual ~SegmentIndex();

	size_t blockSize() { return m_blockSize; }

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
	size_t m_levelCount;
	ScopedArrayPtr<size_t> m_levelKeyCounts;
	ScopedArrayPtr<uint32_t*> m_levelKeys;
};

}

#endif
