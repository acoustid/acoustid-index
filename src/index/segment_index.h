// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_INDEX_H_
#define ACOUSTID_INDEX_SEGMENT_INDEX_H_

#include <QSharedPointer>
#include "common.h"

namespace Acoustid {

class SegmentIndex
{
public:
	SegmentIndex(size_t blockCount);
	virtual ~SegmentIndex();

	size_t blockCount() { return m_blockCount; }

	uint32_t *keys() { return m_keys.get(); }

	uint32_t key(size_t block)
	{
		return m_keys[block];
	}

	bool search(uint32_t key, size_t *firstBlock, size_t *lastBlock);

private:
	size_t m_blockCount;
	std::unique_ptr<uint32_t[]> m_keys;
};

typedef QWeakPointer<SegmentIndex> SegmentIndexWeakPtr;
typedef QSharedPointer<SegmentIndex> SegmentIndexSharedPtr;
typedef QHash<int, SegmentIndexSharedPtr> SegmentIndexMap;

}

#endif
