// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_WRITER_H_
#define ACOUSTID_INDEX_WRITER_H_

#include "common.h"
#include "index_info.h"
#include "index_reader.h"
#include "segment_merge_policy.h"

namespace Acoustid {

class Index;
class InMemoryIndex;
class InMemoryIndexSnapshot;
class SegmentDataWriter;

class IndexWriter : public IndexReader {
 public:
    IndexWriter(IndexSharedPtr index, bool alreadyHasLock = false);
    virtual ~IndexWriter();

    SegmentMergePolicy *segmentMergePolicy() { return m_mergePolicy.get(); }

    void cleanup();
    void optimize();

    void writeSegment(const std::shared_ptr<InMemoryIndex> &index);
    void commit();

 private:
    void flush();
    void maybeFlush();
    void maybeMerge();
    void merge(const QList<int> &merge);

    SegmentDataWriter *segmentDataWriter(const SegmentInfo &info);

    void saveSegmentDocs(SegmentInfo &segment, const std::shared_ptr<SegmentDocs> &docs);

    uint32_t m_maxDocumentId;
    std::unique_ptr<SegmentMergePolicy> m_mergePolicy;
};

}  // namespace Acoustid

#endif
