#pragma once

#include <cstdint>
#include <vector>

#include "fpindex/search_result.h"
#include "fpindex/proto/internal.pb.h"

namespace fpindex {

class BaseSegment {
 public:
    BaseSegment() = delete;
    BaseSegment(const BaseSegment &) = delete;
    BaseSegment &operator=(const BaseSegment &) = delete;
    virtual ~BaseSegment() = default;

    uint32_t id() const { return info_.id(); }

    virtual bool Search(const std::vector<uint32_t> &hashes, std::vector<SearchResult> *results) = 0;
    virtual std::vector<SearchResult> Search(const std::vector<uint32_t> &hashes);

 protected:
    BaseSegment(uint32_t id) {
        info_.set_id(id);
    }

 private:
    SegmentInfo info_;
};

}  // namespace fpindex
