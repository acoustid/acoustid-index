// Copyright (C) 2013  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_DOCUMENT_INDEX_H_
#define ACOUSTID_INDEX_SEGMENT_DOCUMENT_INDEX_H_

#include <QSharedPointer>
#include "common.h"

namespace Acoustid {

class SegmentDocumentIndex
{
public:
	SegmentDocumentIndex(size_t documentCount);
	virtual ~SegmentDocumentIndex();

	// Number of documents in this segment
	size_t documentCount() { return m_documentCount; }

	// Index of the document data file
	uint32_t *ids() { return m_ids.get(); }
	size_t *positions() { return m_positions.get(); }

	// Find position of the given document ID
	bool findPosition(uint32_t id, size_t *position);

private:
	size_t m_documentCount;
	ScopedArrayPtr<uint32_t> m_ids;
	ScopedArrayPtr<size_t> m_positions;
};

typedef QWeakPointer<SegmentDocumentIndex> SegmentDocumentIndexWeakPtr;
typedef QSharedPointer<SegmentDocumentIndex> SegmentDocumentIndexSharedPtr;

}

#endif
