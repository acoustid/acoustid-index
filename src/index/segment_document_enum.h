// Copyright (C) 2013  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_DOCUMENT_ENUM_H_
#define ACOUSTID_INDEX_SEGMENT_DOCUMENT_ENUM_H_

#include "common.h"
#include "segment_document_index.h"
#include "segment_document_reader.h"

namespace Acoustid {

class SegmentDocumentEnum
{
public:
	SegmentDocumentEnum(SegmentDocumentIndexSharedPtr index, SegmentDocumentReader *reader)
		: m_index(index), m_reader(reader), m_document(-1)
	{}

	bool next()
	{
		if (++m_document >= m_index->documentCount()) {
			return false;
		}

		uint32_t *data;
		m_reader->readDocument(m_index->position(m_document), &data, &m_length);

		m_data.reset(data);
		return true;
	}

	uint32_t id()
	{
		return m_index->id(m_document);
	}

	uint32_t *data() { return m_data.get(); }
	size_t length() { return m_length; }

private:
	size_t m_document;
	size_t m_length;
	ScopedArrayPtr<uint32_t> m_data;
	SegmentDocumentIndexSharedPtr m_index;
	ScopedPtr<SegmentDocumentReader> m_reader;
};

}

#endif
