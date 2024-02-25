// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "store/output_stream.h"
#include "util/vint.h"
#include "segment_data_writer.h"
#include "segment_index_writer.h"

using namespace Acoustid;

SegmentDataWriter::SegmentDataWriter(OutputStream *output, SegmentIndexWriter *indexWriter, size_t blockSize)
	: m_output(output), m_indexWriter(indexWriter), m_blockSize(blockSize)
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
	assert(m_itemCount < (1 << 16));
	m_output->writeInt16(m_itemCount);
	m_output->writeBytes(m_buffer.get(), m_blockSize - 2);
	m_ptr = m_buffer.get();
	m_itemCount = 0;
	m_blockCount++;
	memset(m_buffer.get(), 0, m_blockSize);
}

void SegmentDataWriter::addItem(uint32_t key, uint32_t value)
{
	assert(key >= m_lastKey);
	assert(key == m_lastKey ? value >= m_lastValue : 1);

	//qDebug() << "Adding" << key << "to checksum =" << m_checksum;
	m_checksum ^= key;
	m_checksum ^= value;

	if (!m_buffer) {
		m_buffer.reset(new uint8_t[m_blockSize]);
		memset(m_buffer.get(), 0, m_blockSize);
		m_ptr = m_buffer.get();
	}

	uint32_t keyDelta = m_itemCount ? key - m_lastKey : UINT32_MAX;
	uint32_t valueDelta = keyDelta ? value : value - m_lastValue;

	size_t currentSize = m_ptr - m_buffer.get();
	currentSize += 2;
	currentSize += m_itemCount ? checkVInt32Size(keyDelta) : 0;
	currentSize += checkVInt32Size(valueDelta);

	if (currentSize > m_blockSize) {
		writeBlock();
		keyDelta = key;
		valueDelta = value;
	}

	if (m_itemCount) {
		m_ptr += writeVInt32ToArray(m_ptr, keyDelta);
	}
	else {
		m_indexData.push_back(key);
		if (m_indexWriter) {
			m_indexWriter->addItem(key);
		}
	}
	m_ptr += writeVInt32ToArray(m_ptr, valueDelta);

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
	m_index = SegmentIndexSharedPtr(new SegmentIndex(m_blockCount));
	std::copy(m_indexData.begin(), m_indexData.end(), m_index->keys());
	m_indexData.clear();
	m_output->flush();
	m_indexWriter->close();
}

