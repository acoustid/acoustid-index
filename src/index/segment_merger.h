// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_MERGER_H_
#define ACOUSTID_INDEX_SEGMENT_MERGER_H_

#include "common.h"
#include "segment_enum.h"
#include "segment_index_data_writer.h"

namespace Acoustid {

class SegmentMerger
{
public:
	SegmentMerger(SegmentIndexDataWriter *target);
	virtual ~SegmentMerger();

	void addSource(SegmentEnum *reader)
	{
		m_readers.append(reader);
	}

	SegmentIndexDataWriter *writer()
	{
		return m_writer.get();
	}

	size_t merge();

private:
	QList<SegmentEnum *> m_readers;
	ScopedPtr<SegmentIndexDataWriter> m_writer;
};

}

#endif
