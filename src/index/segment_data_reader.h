// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_DATA_READER_H_
#define ACOUSTID_INDEX_SEGMENT_DATA_READER_H_

#include "common.h"
#include "store/input_stream.h"

namespace Acoustid {

class BlockDataIterator
{
public:
	BlockDataIterator(InputStream *input, size_t length, uint32_t firstKey)
		: m_input(input), m_length(length), m_position(0), m_key(firstKey)
	{
	}

	bool next()
	{
		if (m_position++ >= m_length) {
			return false;
		}

		if (m_position == 1) {
			// first item, read only the value
			m_value = m_input->readVInt32();
		}
		else {
			// read both key and value
			uint32_t keyDelta = m_input->readVInt32(); 
			if (keyDelta) {
				m_value = 0;
			}
			m_key += keyDelta;
			m_value += m_input->readVInt32();
		}
		return true;
	}

	uint32_t key() { return m_key; }
	uint32_t value() { return m_value; }

private:
	InputStream *m_input;
	size_t m_length;
	size_t m_position;
	uint32_t m_key, m_value;
};

class SegmentDataReader
{
public:
	SegmentDataReader(InputStream *input, size_t blockSize);
	virtual ~SegmentDataReader();

	size_t blockSize() { return m_blockSize; }
	void setBlockSize(size_t blockSize);

	std::unique_ptr<BlockDataIterator> readBlock(size_t n, uint32_t key);

private:
	std::unique_ptr<InputStream> m_input;
	size_t m_blockSize;
};

}

#endif
