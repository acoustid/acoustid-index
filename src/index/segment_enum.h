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

#ifndef ACOUSTID_INDEX_SEGMENT_ENUM_H_
#define ACOUSTID_INDEX_SEGMENT_ENUM_H_

#include "common.h"
#include "segment_index.h"
#include "segment_data_reader.h"

namespace Acoustid {

class SegmentEnum
{
public:
	SegmentEnum(SegmentIndex *index, SegmentDataReader *dataReader)
		: m_index(index), m_dataReader(dataReader), m_block(0),
		  m_currentBlock(0)
	{}

	bool next()
	{
		if (!m_currentBlock.get() || !m_currentBlock->next()) {
			if (m_block >= m_index->levelKeyCount(0)) {
				return false;
			}
			uint32_t firstKey = m_index->levelKey(m_block);
			m_currentBlock.reset(m_dataReader->readBlock(m_block, firstKey));
			m_currentBlock->next();
			m_block++;
		}
		return true;
	}

	size_t key()
	{
		return m_currentBlock->key();
	}

	size_t value()
	{
		return m_currentBlock->value();
	}

private:
	size_t m_block;
	SegmentIndex *m_index;
	SegmentDataReader *m_dataReader;
	ScopedPtr<BlockDataIterator> m_currentBlock;
};

}

#endif
