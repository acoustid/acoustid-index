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
		: m_index(index), m_dataReader(dataReader), m_block(0),
		  m_currentBlock(0)
	{}

	bool next()
	{
		if (!m_currentBlock.get() || !m_currentBlock->next()) {
			if (m_block >= m_index->blockCount()) {
				return false;
			}
			uint32_t firstKey = m_index->key(m_block);
			m_currentBlock.reset(m_dataReader->readBlock(m_block, firstKey));
			m_currentBlock->next();
			m_block++;
		}
		return true;
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
	size_t m_block;
	SegmentIndexSharedPtr m_index;
	ScopedPtr<SegmentDataReader> m_dataReader;
	ScopedPtr<BlockDataIterator> m_currentBlock;
};

}

#endif
