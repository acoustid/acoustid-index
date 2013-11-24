// Copyright (C) 2013  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "common.h"
#include "acoustid_document_handler.h"

using namespace Acoustid;

static const uint32_t SILENCE = 627964279;

#define MATCH_BITS 14
#define MATCH_MASK ((1 << MATCH_BITS) - 1)
#define MATCH_STRIP(x) ((uint32_t)(x) >> (32 - MATCH_BITS))

#define UNIQ_BITS 16
#define UNIQ_MASK ((1 << MATCH_BITS) - 1)
#define UNIQ_STRIP(x) ((uint32_t)(x) >> (32 - MATCH_BITS))

AcoustIdDocumentHandler::AcoustIdDocumentHandler(int queryStart, int querySize, int queryBits)
	: m_queryStart(queryStart), m_querySize(querySize), m_queryBits(queryBits)
{
	m_queryBitMask = ((1 << queryBits) - 1) << (32 - queryBits);
}

Document AcoustIdDocumentHandler::extractQuery(const Document &doc)
{
	int cleanSize = 0;
	for (int i = 0; i < doc.size(); i++) {
		if (doc[i] != SILENCE) {
			cleanSize++;
		}
	}

	Document query(m_querySize);
	int querySize = 0;
	int start = qMax(0, qMin(cleanSize - m_querySize, m_queryStart));
	for (int i = start; i < doc.size() && querySize < m_querySize; i++) {
		if (doc[i] != SILENCE) {
			uint32_t x = doc[i] & m_queryBitMask;
			if (query.indexOf(x) == -1) {
				query[querySize++] = x;
			}
		}
	}
	query.resize(querySize);

	return query;
}

bool AcoustIdDocumentHandler::canCompare() const
{
	return true;
}

float AcoustIdDocumentHandler::compare(const Document &doc1, const Document &doc2)
{
	return 0.0;
}

