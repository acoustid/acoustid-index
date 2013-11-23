// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "top_hits_collector.h"

using namespace Acoustid;

TopHitsCollector::TopHitsCollector(size_t numHits, int topScorePercent)
	: m_numHits(numHits), m_topScorePercent(topScorePercent)
{
}

TopHitsCollector::~TopHitsCollector()
{
}

void TopHitsCollector::collect(uint32_t id)
{
	m_counts[id] = m_counts[id] + 1;
}

struct CompareByCount
{
	CompareByCount(const QHash<uint32_t, unsigned int> &counts) : m_counts(counts) {}
	bool operator()(uint32_t a, uint32_t b)
	{
		return m_counts[a] > m_counts[b];
	}
	const QHash<uint32_t, unsigned int> &m_counts;
};

QList<Result> TopHitsCollector::topResults()
{
	QList<uint32_t> ids = m_counts.keys();
	QList<Result> results;
	if (ids.isEmpty()) {
		return results;
	}
	qSort(ids.begin(), ids.end(), CompareByCount(m_counts));
	unsigned int minScore = (50 + m_counts[ids.first()] * m_topScorePercent) / 100;
	for (int i = 0; i < std::min(m_numHits, size_t(ids.size())); i++) {
		uint32_t id = ids.at(i);
		unsigned int score = m_counts[id];
		if (score < minScore) {
			break;
		}
		results.append(Result(id, score));
	}
	return results;
}

