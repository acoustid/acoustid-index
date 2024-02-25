// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_ENUM_H_
#define ACOUSTID_INDEX_SEGMENT_ENUM_H_

#include "common.h"
#include "segment_index.h"
#include "segment_data_reader.h"

namespace Acoustid {

class SegmentEnum
{
public:
	SegmentEnum(SegmentIndexSharedPtr index, SegmentDataReader *dataReader)
		: m_index(index), m_dataReader(std::move(dataReader))
	{}

    void setFilter(const QSet<uint32_t> &excludeDocIds) { m_excludeDocIds = excludeDocIds; }

	bool next()
	{
        while (true) {
            if (!m_currentBlock.get() || !m_currentBlock->next()) {
                if (m_block >= m_index->blockCount()) {
                    return false;
                }
                uint32_t firstKey = m_index->key(m_block);
                m_currentBlock = m_dataReader->readBlock(m_block, firstKey);
                m_currentBlock->next();
                m_block++;
            }
            if (m_excludeDocIds.contains(m_currentBlock->value())) {
                continue;
            }
            return true;
        }
	}

	uint32_t key()
	{
		return m_currentBlock->key();
	}

	uint32_t value()
	{
		return m_currentBlock->value();
	}

private:
	size_t m_block{0};
	SegmentIndexSharedPtr m_index;
	std::unique_ptr<SegmentDataReader> m_dataReader;
	std::unique_ptr<BlockDataIterator> m_currentBlock;
  QSet<uint32_t> m_excludeDocIds;
};

}

#endif
