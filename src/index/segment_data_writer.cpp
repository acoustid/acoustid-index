// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "store/output_stream.h"
#include "segment_data_writer.h"
#include "segment_index_writer.h"

using namespace Acoustid;

inline size_t checkVInt32Size(uint32_t i)
{
	if (i < 128) {
		return 1;
	}
	if (i < 128 * 128) {
		return 2;
	}
	if (i < 128 * 128 * 128) {
		return 3;
	}
	if (i < 128 * 128 * 128 * 128) {
		return 4;
	}
	return 5;
}

inline uint8_t *encodeVInt32(uint32_t i, uint8_t *dest)
{
	while (i & ~0x7f) {
		*dest++ = (i & 0x7f) | 0x80;
		i >>= 7;
	}
	*dest++ = i;
	return dest;
}

SegmentDataWriter::SegmentDataWriter(OutputStream *output, SegmentIndexWriter *indexWriter, size_t blockSize)
	: m_output(output), m_indexWriter(indexWriter), m_blockSize(blockSize),
	  m_buffer(0), m_ptr(0), m_itemCount(0), m_lastKey(0), m_lastValue(0)
{
}

SegmentDataWriter::~SegmentDataWriter()
{
	close();
}

void SegmentDataWriter::setBlockSize(size_t blockSize)
{
	m_buffer.reset();
	m_blockSize = blockSize;
}

void SegmentDataWriter::writeBlock()
{
	m_output->writeVInt32(m_itemCount);
	m_output->writeBytes(m_buffer.get(), m_blockSize - checkVInt32Size(m_itemCount)); // XXX
	m_ptr = m_buffer.get();
	m_itemCount = 0;
	memset(m_buffer.get(), 0, m_blockSize);
}

void SegmentDataWriter::addItem(uint32_t key, uint32_t value)
{
	if (!m_buffer) {
		m_buffer.reset(new uint8_t[m_blockSize]);
		memset(m_buffer.get(), 0, m_blockSize);
		m_ptr = m_buffer.get();
	}

	uint32_t keyDelta = m_itemCount ? key - m_lastKey : ~0;
	uint32_t valueDelta = keyDelta ? value : value - m_lastValue;

	size_t currentSize = m_ptr - m_buffer.get();
	currentSize += checkVInt32Size(m_itemCount + 1);
	currentSize += m_itemCount ? checkVInt32Size(keyDelta) : 0;
	currentSize += checkVInt32Size(valueDelta);

	if (currentSize > m_blockSize) {
		writeBlock();
		keyDelta = key;
		valueDelta = value;
	}

	if (m_itemCount > 0) {
		m_ptr = encodeVInt32(keyDelta, m_ptr);
	}
	else if (m_indexWriter) {
		m_indexWriter->addItem(key);
	}
	m_ptr = encodeVInt32(valueDelta, m_ptr);

	m_lastKey = key;
	m_lastValue = value;
	m_itemCount++;

	if (currentSize == m_blockSize) {
		writeBlock();
	}
}

void SegmentDataWriter::close()
{
	if (m_itemCount) {
		writeBlock();
	}
	m_output->flush();
}

