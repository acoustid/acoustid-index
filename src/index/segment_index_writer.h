// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_INDEX_WRITER_H_
#define ACOUSTID_INDEX_SEGMENT_INDEX_WRITER_H_

#include <QList>
#include "common.h"

namespace Acoustid {

class OutputStream;

class SegmentIndexWriter
{
public:
	SegmentIndexWriter(OutputStream *output);
	virtual ~SegmentIndexWriter();

	size_t blockSize() const { return m_blockSize; }
	void setBlockSize(size_t i) { m_blockSize = i; }

	void addItem(uint32_t key);
	void close();

private:
	void maybeWriteHeader();

	size_t m_blockSize;
	uint32_t m_lastKey;
	size_t m_keyCount;
	size_t m_keyCountPosition;
	bool m_headerWritten;
	OutputStream *m_output;
};

}

#endif
