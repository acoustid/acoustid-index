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
		: m_index(index), m_reader(reader), m_document_no(-1)
	{}

	bool next()
	{
		if (++m_document_no >= m_index->documentCount()) {
			return false;
		}

		m_reader->readDocument(m_index->position(m_document_no), &m_document);
		return true;
	}

	uint32_t id()
	{
		return m_index->id(m_document_no);
	}

	Document document()
	{
		return m_document;
	}

private:
	size_t m_document_no;
	Document m_document;
	SegmentDocumentIndexSharedPtr m_index;
	ScopedPtr<SegmentDocumentReader> m_reader;
};

}

#endif
