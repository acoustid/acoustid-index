#pragma once

#include <atomic>
#include <map>
#include <shared_mutex>
#include <vector>

#include "fpindex/base_segment.h"
#include "fpindex/io/file.h"

namespace fpindex {

class SegmentBuilder : public BaseSegment {
 public:
    SegmentBuilder(uint32_t id) : BaseSegment(id) {}
    SegmentBuilder(const SegmentBuilder&) = delete;
    SegmentBuilder& operator=(const SegmentBuilder&) = delete;

    bool Add(uint32_t id, const std::vector<uint32_t>& values);

    bool Search(const std::vector<uint32_t>& hashes, std::vector<SearchResult>* results) override;

    // Freeze the segment so that no more data can be added to it.
    void Freeze();
    bool IsFrozen();

    // Serialize the segment data to the output stream.
    bool Save(const std::shared_ptr<io::File> &file);

 private:
    std::shared_mutex mutex_;
    std::multimap<uint32_t, uint32_t> data_;
    std::atomic<bool> frozen_{false};
};

}  // namespace fpindex
