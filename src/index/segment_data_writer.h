// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_DATA_WRITER_H_
#define ACOUSTID_INDEX_SEGMENT_DATA_WRITER_H_

#include "common.h"

namespace Acoustid {

class OutputStream;
class SegmentIndexWriter;

typedef QPair<uint32_t, uint32_t> Int32Pair;

class SegmentDataWriter
{
public:
	SegmentDataWriter(OutputStream *output, SegmentIndexWriter *indexWriter, size_t blockSize);
	virtual ~SegmentDataWriter();

	// Number of blocks written into the file.
	size_t blockCount() const { return m_blockCount; }

	// Last key written into the file.
	uint32_t lastKey() const { return m_lastKey; }

	size_t blockSize() { return m_blockSize; }
	void setBlockSize(size_t blockSize);

	void addItem(uint32_t key, uint32_t value);
	void close();

private:
	void writeBlock();

	OutputStream *m_output;
	SegmentIndexWriter *m_indexWriter;
	size_t m_blockSize;
	uint32_t m_lastKey;
	uint32_t m_lastValue;
	size_t m_itemCount;
	size_t m_blockCount;
	uint8_t *m_ptr;
	ScopedArrayPtr<uint8_t> m_buffer;
};

}

#endif
