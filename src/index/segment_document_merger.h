// Copyright (C) 2013  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_DOCUMENT_MERGER_H_
#define ACOUSTID_INDEX_SEGMENT_DOCUMENT_MERGER_H_

#include "common.h"
#include "segment_document_enum.h"
#include "segment_document_writer.h"

namespace Acoustid {

class SegmentDocumentMerger
{
public:
	SegmentDocumentMerger(SegmentDocumentWriter *target);
	virtual ~SegmentDocumentMerger();

	void addSource(SegmentDocumentEnum *reader)
	{
		m_readers.append(reader);
	}

	SegmentDocumentWriter *writer()
	{
		return m_writer.get();
	}

	size_t merge();

private:
	QList<SegmentDocumentEnum *> m_readers;
	ScopedPtr<SegmentDocumentWriter> m_writer;
};

}

#endif
