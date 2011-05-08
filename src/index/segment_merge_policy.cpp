// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <math.h>
#include "segment_merge_policy.h"

// Based on Michael McCandless' TieredMergePolicy for Lucene
// https://issues.apache.org/jira/browse/LUCENE-854

using namespace Acoustid;

SegmentMergePolicy::SegmentMergePolicy(int maxMergeAtOnce, int maxSegmentsPerTier)
{
	setMaxMergeAtOnce(maxMergeAtOnce);
	setMaxSegmentsPerTier(maxSegmentsPerTier);
}

SegmentMergePolicy::~SegmentMergePolicy()
{
}

class SegmentSizeLessThan
{
public:
	SegmentSizeLessThan(const SegmentInfoList *infos)
		: m_infos(infos)
	{
	}

	bool operator()(int a, int b) const
	{
		return m_infos->info(a).blockCount() > m_infos->info(b).blockCount();
	}

private:
	const SegmentInfoList *m_infos;
};

QList<int> SegmentMergePolicy::findMerges(const SegmentInfoList &infos)
{
	if (!infos.size()) {
		return QList<int>();
	}

	QList<int> segments;
	for (size_t i = 0; i < infos.size(); i++) {
		segments.append(i);
	}
	qStableSort(segments.begin(), segments.end(), SegmentSizeLessThan(&infos));
	//qDebug() << "Order after sorting is " << segments;

	size_t minSegmentSize = infos.info(segments.last()).blockCount();
	size_t totalIndexSize = 0;
	for (size_t i = 0; i < infos.size(); i++) {
		totalIndexSize += infos.info(i).blockCount();
	}
	//qDebug() << "minSegmentSize =" << minSegmentSize;
	//qDebug() << "totalIndexSize =" << totalIndexSize;

	size_t levelSize = minSegmentSize;
	size_t indexSize = totalIndexSize;
	size_t allowedSegmentCount = 0;
	while (true) {
		size_t levelSegmentCount = indexSize / levelSize;
		//qDebug() << "levelSize =" << levelSize;
		//qDebug() << "levelSegmentCount =" << levelSegmentCount;
		if (levelSegmentCount < m_maxSegmentsPerTier) {
			allowedSegmentCount += levelSegmentCount;
			break;
		}
		allowedSegmentCount += m_maxSegmentsPerTier;
		indexSize -= m_maxSegmentsPerTier * levelSize;
		levelSize *= m_maxMergeAtOnce;
	}
	//qDebug() << "allowedSegmentCount =" << allowedSegmentCount;

	if (segments.size() <= allowedSegmentCount) {
		return QList<int>();
	}

	QList<int> best;
	double bestScore = 1.0;
	for (size_t i = 0; i <= segments.size() - m_maxMergeAtOnce; i++) {
		size_t mergeSize = 0;
		QList<int> candidate;
		for (size_t j = i; j < segments.size() && candidate.size() < m_maxMergeAtOnce; j++) {
			int segment = segments.at(j);
			candidate.append(segment);
			mergeSize += infos.info(segment).blockCount();
		}
		if (candidate.size()) {
			double score = double(infos.info(candidate.first()).blockCount()) / mergeSize;
			score *= pow(mergeSize, 0.05);
			//qDebug() << "Evaluating merge " << candidate << " with score " << score;
	 		if (score < bestScore) {
				best = candidate;
				bestScore = score;
			}
		}
	}

	//qDebug() << "Best merge is " << best << " with score " << bestScore;
	return best;
}

