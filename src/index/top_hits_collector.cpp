// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <algorithm>
#include "top_hits_collector.h"

using namespace Acoustid;

TopHitsCollector::TopHitsCollector(size_t numHits)
	: m_numHits(numHits)
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
	std::sort(ids.begin(), ids.end(), CompareByCount(m_counts));
	QList<Result> results;
	for (int i = 0; i < std::min(m_numHits, size_t(ids.size())); i++) {
		uint32_t id = ids.at(i);
		results.append(Result(id, m_counts[id]));
	}
	return results;
}

