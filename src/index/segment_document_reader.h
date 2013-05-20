// Copyright (C) 2013  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_DOCUMENT_READER_H_
#define ACOUSTID_INDEX_SEGMENT_DOCUMENT_READER_H_

#include "common.h"
#include "segment_document_index.h"

namespace Acoustid {

class InputStream;

class SegmentDocumentReader
{
public:
	SegmentDocumentReader(InputStream *input);
	virtual ~SegmentDocumentReader();

	void readDocument(size_t position, uint32_t **data, size_t *length);

	static SegmentDocumentIndexSharedPtr readIndex(InputStream *input, size_t documentCount);

private:
	ScopedPtr<InputStream> m_input;
};

}

#endif
