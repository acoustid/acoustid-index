#include "fpindex/segment.h"

#include "fpindex/proto/internal.pb.h"
#include "fpindex/search_result.h"
#include "fpindex/segment_file_format.h"
#include "fpindex/io/file.h"

#include <QDebug>

namespace fpindex {

struct CompareHashAgainstBlockData {
    bool operator()(const std::pair<uint32_t, uint32_t>& item, uint32_t key) const { return item.first < key; }
    bool operator()(uint32_t key, const std::pair<uint32_t, uint32_t>& item) const { return key < item.first; }
};

struct CompareHashAgainstBlockIndexFront {
    bool operator()(const std::pair<uint32_t, uint32_t>& a, uint32_t b) const { return a.first < b; }
    bool operator()(uint32_t a, const std::pair<uint32_t, uint32_t>& b) const { return a < b.first; }
};

struct CompareHashAgainstBlockIndexBack {
    bool operator()(const std::pair<uint32_t, uint32_t>& a, uint32_t b) const { return a.second < b; }
    bool operator()(uint32_t a, const std::pair<uint32_t, uint32_t>& b) const { return a < b.second; }
};

bool BlockBasedSegment::Search(const std::vector<uint32_t>& hashes, std::vector<SearchResult>* results) {
    const auto& block_index = GetBlockIndex();
    if (block_index.empty()) {
        return true;
    }

    std::map<uint32_t, uint32_t> scores;

    std::vector<std::pair<uint32_t, uint32_t>> block_data;
    size_t prev_block_no = SIZE_MAX;
    auto prev_block_range_start = block_index.begin();

    for (const auto& hash : hashes) {
        auto block_it = std::lower_bound(prev_block_range_start, block_index.end(), hash, CompareHashAgainstBlockIndexBack{});
        if (block_it == block_index.end()) {
            block_it = prev_block_range_start;
        }
        prev_block_range_start = block_it;

        while (block_it != block_index.end() && block_it->first <= hash) {
            const size_t block_no = block_it - block_index.begin();
            if (prev_block_no != block_no) {
                if (!GetBlock(block_no, &block_data)) {
                    return false;
                }
                prev_block_no = block_no;
            }
            auto matches = std::equal_range(block_data.begin(), block_data.end(), hash, CompareHashAgainstBlockData{});
            for (auto i = matches.first; i != matches.second; ++i) {
                scores[i->second]++;
            }
            ++block_it;
        }
    }

    results->clear();
    results->reserve(scores.size());
    for (auto score : scores) {
        results->emplace_back(score.first, score.second);
    }
    return true;
}

bool Segment::Search(const std::vector<uint32_t>& hashes, std::vector<SearchResult>* results) {
    if (!ready_) {
        return false;
    }
    return BlockBasedSegment::Search(hashes, results);
}

const std::vector<std::pair<uint32_t, uint32_t>>& Segment::GetBlockIndex() { return block_index_; }

bool Segment::GetBlock(size_t block_no, std::vector<std::pair<uint32_t, uint32_t>>* items) {
    if (block_no >= block_index_.size()) {
        return false;
    }
    const size_t block_offset = first_block_offset_ + block_no * header_.block_size();
    auto coded_stream = file_->GetCodedInputStream(block_offset, header_.block_size());
    return internal::ParseSegmentBlock(coded_stream.get(), header_, items);
}

bool Segment::Load(const std::shared_ptr<io::File>& file) {
    if (ready_) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);

    auto stream = file->GetInputStream();
    auto coded_stream = std::make_unique<google::protobuf::io::CodedInputStream>(stream.get());

    if (!internal::ParseSegmentHeader(coded_stream.get(), &header_)) {
        return false;
    }

    const size_t file_size = file->Size();
    const size_t header_size = coded_stream->CurrentPosition();

    if (header_size > file_size) {
        return false;
    }

    const size_t block_size = header_.block_size();
    const size_t block_count = (file_size - header_size) / block_size;

    if (block_count * block_size + header_size != file->Size()) {
        return false;
    }

    first_block_offset_ = header_size;

    block_index_.clear();
    block_index_.reserve(block_count);

    std::vector<std::pair<uint32_t, uint32_t>> items;
    for (size_t i = 0; i < block_count; ++i) {
        if (!internal::ParseSegmentBlock(coded_stream.get(), header_, &items)) {
            return false;
        }
        if (items.empty()) {
            return false;
        }
        block_index_.emplace_back(items.front().first, items.back().first);
    }

    if (block_index_.size() != block_count) {
        return false;
    }

    file_ = file;
    ready_ = true;
    return true;
}

Segment::Segment(uint32_t id, std::shared_ptr<io::File> file, SegmentHeader header, size_t first_block_offset,
                 std::vector<std::pair<uint32_t, uint32_t>>&& block_index)
    : BlockBasedSegment(id), header_(header), file_(file), first_block_offset_(first_block_offset), block_index_(std::move(block_index)) {
        ready_ = true;
}

}  // namespace fpindex
