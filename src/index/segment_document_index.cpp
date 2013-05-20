// Copyright (C) 2013  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "util/search_utils.h"
#include "segment_document_index.h"

using namespace Acoustid;

SegmentDocumentIndex::SegmentDocumentIndex(size_t documentCount)
	: m_documentCount(documentCount),
	  m_ids(new uint32_t[documentCount]),
	  m_positions(new size_t[documentCount])
{
}

SegmentDocumentIndex::~SegmentDocumentIndex()
{
}

bool SegmentDocumentIndex::findPosition(uint32_t id, size_t *position)
{
	ssize_t pos = searchFirstSmaller(m_ids.get(), 0, m_documentCount, id) + 1;
	if (pos >= m_documentCount || m_ids[pos] != id) {
		return false;
	}
	*position = m_positions[pos];
	return true;
}

