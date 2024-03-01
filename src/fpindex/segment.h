#pragma once

#include <cstddef>

#include "fpindex/base_segment.h"

namespace fpindex {

class Segment : public BaseSegment {
 public:
    Segment() = delete;
    Segment(const Segment &) = delete;
    Segment &operator=(const Segment &) = delete;

    void Search(const std::vector<uint32_t> &hashes, std::vector<SearchResult> *results) override;

 private:
    // pass
};

}  // namespace fpindex
