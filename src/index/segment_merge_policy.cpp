// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "segment_merge_policy.h"

#include <math.h>

// Based on Michael McCandless' TieredMergePolicy for Lucene
// https://issues.apache.org/jira/browse/LUCENE-854

using namespace Acoustid;

SegmentMergePolicy::SegmentMergePolicy(int maxMergeAtOnce, int maxSegmentsPerTier, int maxSegmentBlocks) {
    setMaxMergeAtOnce(maxMergeAtOnce);
    setMaxSegmentsPerTier(maxSegmentsPerTier);
    setMaxSegmentBlocks(maxSegmentBlocks);
    setFloorSegmentBlocks(FLOOR_SEGMENT_BLOCKS);
}

SegmentMergePolicy::~SegmentMergePolicy() {}

class SegmentSizeLessThan {
 public:
    SegmentSizeLessThan(const SegmentInfoList* infos) : m_infos(infos) {}

    bool operator()(int a, int b) const { return m_infos->at(a).blockCount() > m_infos->at(b).blockCount(); }

 private:
    const SegmentInfoList* m_infos;
};

QList<int> SegmentMergePolicy::findMerges(const SegmentInfoList& infos) {
    if (!infos.size()) {
        return QList<int>();
    }

    QList<int> segments;
    for (size_t i = 0; i < infos.size(); i++) {
        segments.append(i);
    }
    qStableSort(segments.begin(), segments.end(), SegmentSizeLessThan(&infos));
    // qDebug() << "Order after sorting is " << segments;

    size_t minSegmentSize = infos.at(segments.last()).blockCount();
    size_t totalIndexSize = 0;
    size_t tooBigCount = 0;
    for (size_t i = 0; i < infos.size(); i++) {
        size_t blockCount = infos.at(i).blockCount();
        if (blockCount <= m_maxSegmentBlocks / 2) {
            totalIndexSize += blockCount;
        } else {
            tooBigCount++;
        }
    }
    // qDebug() << "minSegmentSize =" << minSegmentSize;
    // qDebug() << "totalIndexSize =" << totalIndexSize;

    size_t levelSize = floorSize(minSegmentSize);
    size_t indexSize = totalIndexSize;
    size_t allowedSegmentCount = 0;
    while (true) {
        size_t levelSegmentCount = indexSize / levelSize;
        // qDebug() << "levelSize =" << levelSize;
        // qDebug() << "levelSegmentCount =" << levelSegmentCount;
        if (levelSegmentCount < m_maxSegmentsPerTier) {
            allowedSegmentCount += levelSegmentCount;
            break;
        }
        allowedSegmentCount += m_maxSegmentsPerTier;
        indexSize -= m_maxSegmentsPerTier * levelSize;
        levelSize *= m_maxMergeAtOnce;
    }
    // qDebug() << "allowedSegmentCount =" << allowedSegmentCount;

    if (segments.size() <= allowedSegmentCount) {
        return QList<int>();
    }

    QList<int> best;
    double bestScore = 1.0;
    size_t numPossibleCandidates = qMax(0, segments.size() - m_maxMergeAtOnce);
    for (size_t i = tooBigCount; i <= numPossibleCandidates; i++) {
        size_t mergeSize = 0;
        size_t mergeSizeFloored = 0;
        QList<int> candidate;
        for (size_t j = i; j < segments.size() && candidate.size() < m_maxMergeAtOnce; j++) {
            int segment = segments.at(j);
            size_t segBlockCount = infos.at(segment).blockCount();
            if (mergeSize + segBlockCount > m_maxSegmentBlocks) {
                continue;
            }
            candidate.append(segment);
            mergeSize += segBlockCount;
            mergeSizeFloored += floorSize(segBlockCount);
        }
        if (candidate.size()) {
            double score = double(floorSize(infos.at(candidate.first()).blockCount())) / mergeSizeFloored;
            score *= pow(mergeSize, 0.05);
            // qDebug() << "Evaluating merge " << candidate << " with score " <<
            // score;
            if (score < bestScore) {
                best = candidate;
                bestScore = score;
            }
        }
    }

    // qDebug() << "Best merge is " << best << " with score " << bestScore;
    return best;
}
