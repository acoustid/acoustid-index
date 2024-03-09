#pragma once

#include <atomic>
#include <map>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "fpindex/base_segment.h"
#include "fpindex/io/file.h"

namespace fpindex {

class Segment;

enum class DocStatus {
    UPDATED = 0,
    DELETED = 1,
};

class SegmentBuilder : public BaseSegment {
 public:
    SegmentBuilder(uint32_t id) : BaseSegment(id) {}
    SegmentBuilder(const SegmentBuilder&) = delete;
    SegmentBuilder& operator=(const SegmentBuilder&) = delete;

    bool InsertOrUpdate(uint32_t id, const google::protobuf::RepeatedField<uint32_t>& values);
    bool InsertOrUpdate(uint32_t id, const std::vector<uint32_t>& values);
    bool Delete(uint32_t id);

    bool Update(const std::vector<OplogEntry>& update);

    bool Contains(uint32_t id);

    bool Search(const std::vector<uint32_t>& hashes, std::vector<SearchResult>* results) override;

    // Freeze the segment so that no more data can be added to it.
    void Freeze();
    bool IsFrozen();

    // Serialize the segment data to the output stream.
    std::shared_ptr<Segment> Save(const std::shared_ptr<io::File>& file);

 private:
    void DeleteInternal(uint32_t id);

    std::shared_mutex mutex_;
    std::multimap<uint32_t, uint32_t> data_;
    std::unordered_set<uint32_t> ids_;
    std::unordered_map<uint32_t, DocStatus> updates_;
    std::atomic<bool> frozen_{false};
};

}  // namespace fpindex
