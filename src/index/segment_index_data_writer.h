// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_DATA_WRITER_H_
#define ACOUSTID_INDEX_SEGMENT_DATA_WRITER_H_

#include "common.h"
#include "segment_index.h"

namespace Acoustid {

class OutputStream;
class SegmentIndexWriter;

typedef QPair<uint32_t, uint32_t> Int32Pair;

class SegmentIndexDataWriter
{
public:
	SegmentIndexDataWriter(OutputStream *indexOutput, OutputStream *dataOutput, size_t blockSize);
	virtual ~SegmentIndexDataWriter();

	// Number of blocks written into the file.
	size_t blockCount() const { return m_blockCount; }

	// Last key written into the file.
	uint32_t lastKey() const { return m_lastKey; }

	uint32_t checksum() const { return m_checksum; }

	SegmentIndexSharedPtr index() const { return m_index; }

	size_t blockSize() { return m_blockSize; }
	void setBlockSize(size_t blockSize);

	void addItem(uint32_t key, uint32_t value);
	void close();

private:
	void writeBlock();

	ScopedPtr<OutputStream> m_indexOutput;
	ScopedPtr<OutputStream> m_dataOutput;
	SegmentIndexSharedPtr m_index;
	QList<uint32_t> m_indexData;
	size_t m_blockSize;
	uint32_t m_lastKey;
	uint32_t m_lastValue;
	uint32_t m_checksum;
	size_t m_itemCount;
	size_t m_blockCount;
	uint8_t *m_ptr;
	ScopedArrayPtr<uint8_t> m_buffer;
};

}

#endif
