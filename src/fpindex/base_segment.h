#pragma once

#include <cstdint>
#include <vector>

namespace fpindex {

class SearchResult;

class BaseSegment {
 public:
    BaseSegment() = delete;
    BaseSegment(const BaseSegment &) = delete;
    BaseSegment &operator=(const BaseSegment &) = delete;
    virtual ~BaseSegment() = default;

    uint32_t id() const { return id_; }

    virtual void Search(const std::vector<uint32_t> &hashes, std::vector<SearchResult> *results) = 0;
    std::vector<SearchResult> Search(const std::vector<uint32_t> &hashes);

 protected:
    BaseSegment(uint32_t id) : id_(id) {}

 private:
    uint32_t id_{0};
};

}  // namespace fpindex
