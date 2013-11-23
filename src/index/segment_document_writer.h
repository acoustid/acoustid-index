// Copyright (C) 2013  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_DOCUMENT_WRITER_H_
#define ACOUSTID_INDEX_SEGMENT_DOCUMENT_WRITER_H_

#include "common.h"
#include "document.h"
#include "segment_document_index.h"

namespace Acoustid {

class OutputStream;

class SegmentDocumentWriter
{
public:
	SegmentDocumentWriter(OutputStream *indexOutput, OutputStream *dataOutput);
	virtual ~SegmentDocumentWriter();

	uint32_t checksum() const { return m_checksum; }
	size_t documentCount() const { return m_positions.size(); }
	SegmentDocumentIndexSharedPtr index() const { return m_index; }

	void addDocument(uint32_t id, const Document &doc);
	void close();

private:
	ScopedPtr<OutputStream> m_indexOutput;
	ScopedPtr<OutputStream> m_dataOutput;
	QMap<uint32_t, size_t> m_positions;
	uint32_t m_checksum;
	SegmentDocumentIndexSharedPtr m_index;
};

}

#endif
