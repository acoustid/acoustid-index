// Copyright (C) 2013  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_ACOUSTID_DOCUMENT_HANDLER_H_
#define ACOUSTID_INDEX_ACOUSTID_DOCUMENT_HANDLER_H_

#include "common.h"
#include "document.h"
#include "document_handler.h"

namespace Acoustid {

class AcoustIdDocumentHandler : public DocumentHandler
{
public:
	AcoustIdDocumentHandler(int queryStart = QUERY_START, int querySize = QUERY_SIZE, int queryBits = QUERY_BITS);

	// Extract a part of the document that should be indexed.
	Document extractQuery(const Document &doc);

	// Whether this handler can compare full documents. If not, the number
	// of matched items from the indexed query is used as a score.
	bool canCompare() const;

	// Compare two full documents and return the score.
	float compare(const Document &doc1, const Document &doc2);

private:
	static const int QUERY_START = 80;
	static const int QUERY_SIZE = 120;
	static const int QUERY_BITS = 28;

	int m_queryStart;
	int m_querySize;
	int m_queryBits;
	int m_queryBitMask;
	QVector<uint16_t> m_offsets1;
	QVector<uint16_t> m_offsets2;
	QVector<unsigned char> m_seen;
	QVector<unsigned short> m_counts;
};

}

#endif
