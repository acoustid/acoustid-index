// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_MERGE_POLICY_H_
#define ACOUSTID_INDEX_SEGMENT_MERGE_POLICY_H_

#include "common.h"
#include "index_info.h"
#include "segment_info.h"

namespace Acoustid {

class SegmentMergePolicy {
 public:
    SegmentMergePolicy(int maxMergeAtOnce = MAX_MERGE_AT_ONCE, int maxSegmentsPerTier = MAX_SEGMENTS_PER_TIER, int maxSegmentBlocks = MAX_SEGMENT_BLOCKS);
    virtual ~SegmentMergePolicy();

    void setMaxMergeAtOnce(int maxMergeAtOnce) { m_maxMergeAtOnce = maxMergeAtOnce; }

    int maxMergeAtOnce() const { return m_maxMergeAtOnce; }

    void setMaxSegmentsPerTier(int maxSegmentsPerTier) { m_maxSegmentsPerTier = maxSegmentsPerTier; }

    int maxSegmentsPerTier() const { return m_maxSegmentsPerTier; }

    void setMaxSegmentBlocks(int maxSegmentBlocks) { m_maxSegmentBlocks = maxSegmentBlocks; }

    int maxSegmentBlocks() const { return m_maxSegmentBlocks; }

    void setFloorSegmentBlocks(int floorSegmentBlocks) { m_floorSegmentBlocks = floorSegmentBlocks; }

    int floorSegmentBlocks() const { return m_floorSegmentBlocks; }

    QList<int> findMerges(const SegmentInfoList& infos);

 protected:
    int floorSize(int size) const { return std::max(size, m_floorSegmentBlocks); }

 private:
    int m_maxMergeAtOnce;
    int m_maxSegmentsPerTier;
    int m_maxSegmentBlocks;
    int m_floorSegmentBlocks;
};

}  // namespace Acoustid

#endif
