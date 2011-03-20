// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_TOP_HITS_COLLECTOR_H_
#define ACOUSTID_INDEX_TOP_HITS_COLLECTOR_H_

#include <QList>
#include <QHash>
#include "common.h"
#include "collector.h"

namespace Acoustid {

class Result
{
public:
	Result(uint32_t id = 0, double score = 0.0)
		: m_id(id), m_score(score) {}

	uint32_t id() const { return m_id; }
	double score() const { return m_score; }

private:
	uint32_t m_id;
	double m_score;
};

class TopHitsCollector : public Collector
{
public:
	TopHitsCollector(size_t numHits);
	~TopHitsCollector();
	void collect(uint32_t id);

	QList<Result> topResults();

private:
	QHash<uint32_t, unsigned int> m_counts;
	size_t m_numHits;
};

}

#endif

