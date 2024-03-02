#pragma once

#include <atomic>
#include <cstddef>
#include <memory>

#include "fpindex/base_segment.h"
#include "fpindex/proto/internal.pb.h"

namespace fpindex {

namespace io {
class File;
}  // namespace io

class BlockBasedSegment : public BaseSegment {
 public:
    BlockBasedSegment(uint32_t id) : BaseSegment(id) {}
    BlockBasedSegment(const BlockBasedSegment &) = delete;
    BlockBasedSegment &operator=(const BlockBasedSegment &) = delete;

    bool Search(const std::vector<uint32_t> &hashes, std::vector<SearchResult> *results) override;

 protected:
    virtual const std::vector<uint32_t> &GetBlockIndex() = 0;
    virtual bool GetBlock(size_t block_no, std::vector<std::pair<uint32_t, uint32_t>> *items) = 0;
};

class Segment : public BlockBasedSegment {
 public:
    Segment(uint32_t id) : BlockBasedSegment(id) {}
    Segment(const Segment &) = delete;
    Segment &operator=(const Segment &) = delete;

    bool Search(const std::vector<uint32_t> &hashes, std::vector<SearchResult> *results) override;
    bool Load(const std::shared_ptr<io::File> &file);

 protected:
    const std::vector<uint32_t> &GetBlockIndex() override;
    bool GetBlock(size_t block_no, std::vector<std::pair<uint32_t, uint32_t>> *items) override;

 private:
    std::mutex mutex_;
    std::atomic<bool> ready_{false};
    SegmentHeader header_;
    std::shared_ptr<io::File> file_;
    size_t first_block_offset_{0};
    std::vector<uint32_t> block_index_;
};

}  // namespace fpindex
