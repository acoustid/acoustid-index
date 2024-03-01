#pragma once

#include <cstdint>

namespace fpindex {

class SearchResult {
 public:
    SearchResult(uint32_t id, uint32_t score) : id_(id), score_(score) {}

    uint32_t id() const { return id_; }
    uint32_t score() const { return score_; }

 private:
    uint32_t id_;
    uint32_t score_;
};

}  // namespace fpindex
