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

class SegmentDataWriter
{
public:
	SegmentDataWriter(OutputStream *output, SegmentIndexWriter *indexWriter, size_t blockSize);
	virtual ~SegmentDataWriter();

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

	std::unique_ptr<OutputStream> m_output;
	std::unique_ptr<SegmentIndexWriter> m_indexWriter;
	SegmentIndexSharedPtr m_index;
	std::vector<uint32_t> m_indexData;
	size_t m_blockSize{0};
	uint32_t m_lastKey{0};
	uint32_t m_lastValue{0};
	uint32_t m_checksum{0};
	size_t m_itemCount{0};
	size_t m_blockCount{0};
	uint8_t *m_ptr{nullptr};
	std::unique_ptr<uint8_t[]> m_buffer;
};

}

#endif
