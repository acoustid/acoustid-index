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

	void addItem(uint32_t key);
	void close();

private:
	void maybeWriteHeader();

	ScopedPtr<OutputStream> m_output;
};

}

#endif
