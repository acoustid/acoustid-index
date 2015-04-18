// Copyright (C) 2013  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <math.h>
#include "common.h"
#include "util/popcount.h"
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
	m_offsets1.fill(0, MATCH_MASK);
	m_offsets2.fill(0, MATCH_MASK);
	m_counts.fill(0, doc1.size() + doc2.size() + 1);

	int maxOffset = 120; // XXX

	int offset1 = 0;
	int offset2 = 0;
	int size1 = doc1.size();
	int size2 = doc2.size();

	for (int i = 0; i < size1; i++) {
		m_offsets1[MATCH_STRIP(doc1[i])] = i;
	}

	for (int i = 0; i < size2; i++) {
		m_offsets2[MATCH_STRIP(doc2[i])] = i;
	}

	int topCount = 0;
	int topOffset = 0;
	for (int i = 0; i < MATCH_MASK; i++) {
		if (m_offsets1[i] && m_offsets2[i]) {
			int offset = m_offsets1[i] - m_offsets2[i];
			if (maxOffset == 0 || (-maxOffset <= offset && offset <= maxOffset)) {
				offset += size2;
				m_counts[offset]++;
				if (m_counts[offset] > topCount) {
					topCount = m_counts[offset];
					topOffset = offset;
				}
			}
		}
	}
	topOffset -= size2;

	int minSize = qMin(size1, size2) & ~1;

	if (topOffset < 0) {
		offset2 -= topOffset;
		size2 = qMax(0, size2 + topOffset);
	}
	else {
		offset1 += topOffset;
		size1 = qMax(0, size1 - topOffset);
	}

	int size = qMin(size1, size2) / 2;
	if (!size || !minSize) {
		qDebug() << "Empty matching subfingerprint";
		return 0.0;
	}

	int uniq1 = 0;
	int uniq2 = 0;

	m_seen.fill(0, UNIQ_MASK);
	for (int i = 0; i < size1; i++) {
		int key = UNIQ_STRIP(doc1[offset1 + i]);
		if (!m_seen[key]) {
			uniq1++;
			m_seen[key] = 1;
		}
	}

	m_seen.fill(0, UNIQ_MASK);
	for (int i = 0; i < size2; i++) {
		int key = UNIQ_STRIP(doc2[offset2 + i]);
		if (!m_seen[key]) {
			uniq2++;
			m_seen[key] = 1;
		}
	}

	float diversity = qMin(qMin(1.0, (float)(uniq1 + 10) / size1 + 0.5),
	                       qMin(1.0, (float)(uniq2 + 10) / size2 + 0.5));

//	qDebug() << QString("Offset %d, offset score %d, size %d, uniq size %d, diversity %f").arg(topoffset), topcount, size * 2, Max(auniq, buniq), diversity)));

	if (topCount < qMax(uniq1, uniq2) * 0.02) {
		qDebug() << "Top offset score is below 2\% of the unique size";
		return 0.0;
	}

	int bitError = 0;
	const uint64_t *data1 = reinterpret_cast<const uint64_t *>(doc1.constData() + offset1);
	const uint64_t *data2 = reinterpret_cast<const uint64_t *>(doc2.constData() + offset2);
	for (int i = 0; i < size; i++, data1++, data2++) {
		bitError += popCount(*data1 ^ *data2);
	}

	float score = (size * 2.0 / minSize) * (1.0 - 2.0 * (float)bitError / (64 * size));
	if (score < 0.0) {
		score = 0.0;
	}
	if (diversity < 1.0) {
		float newscore = pow(score, 8.0 - 7.0 * diversity);
//		ereport(DEBUG4, (errmsg("acoustid_compare2: scaling score because of duplicate items, %f => %f", score, newscore)));
		score = newscore;
	}

	return score;
}

