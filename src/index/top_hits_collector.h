// Acoustid Index -- Inverted index for audio fingerprints
// Copyright (C) 2011  Lukas Lalinsky
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

#ifndef ACOUSTID_INDEX_TOP_HITS_COLLECTOR_H_
#define ACOUSTID_INDEX_TOP_HITS_COLLECTOR_H_

#include <QList>
#include <QHash>
#include "common.h"
#include "collector.h"

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

#endif

