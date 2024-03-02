#include "fpindex/segment_builder.h"

#include <google/protobuf/io/coded_stream.h>

#include <mutex>

#include "fpindex/search_result.h"
#include "fpindex/segment_file_format.h"

namespace fpindex {

bool SegmentBuilder::Add(uint32_t id, const std::vector<uint32_t>& hashes) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (frozen_) {
        return false;
    }
    for (auto hash : hashes) {
        data_.insert(std::make_pair(id, hash));
    }
    return true;
}

bool SegmentBuilder::IsFrozen() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    return frozen_;
}

void SegmentBuilder::Freeze() {
    if (!frozen_) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        frozen_ = true;
    }
}

bool SegmentBuilder::Search(const std::vector<uint32_t>& hashes, std::vector<SearchResult>* results) {
    std::shared_lock<std::shared_mutex> lock;
    if (!frozen_) {
        lock = std::shared_lock<std::shared_mutex>(mutex_);
    }
    std::map<uint32_t, uint32_t> scores;
    for (auto hash : hashes) {
        auto range = data_.equal_range(hash);
        for (auto it = range.first; it != range.second; ++it) {
            scores[it->first]++;
        }
    }
    results->clear();
    results->reserve(scores.size());
    for (auto score : scores) {
        results->emplace_back(score.first, score.second);
    }
    return true;
}

bool SegmentBuilder::Serialize(io::File* file) {
    std::shared_lock<std::shared_mutex> lock;
    if (!frozen_) {
        lock = std::shared_lock<std::shared_mutex>(mutex_);
    }

    auto stream = file->GetOutputStream();
    auto coded_stream = std::make_unique<google::protobuf::io::CodedOutputStream>(stream.get());

    SegmentHeader header;
    internal::InitializeSegmentHeader(&header);
    internal::SerializeSegmentHeader(coded_stream.get(), header);
    if (coded_stream->HadError()) {
        return false;
    }

    const int block_size = header.block_size();
    const int header_size = coded_stream->ByteCount();

    int block_count = 0;
    auto it = data_.begin();
    while (it != data_.end()) {
        it = internal::SerializeSegmentBlock(coded_stream.get(), header, it, data_.end());
        if (coded_stream->HadError()) {
            return false;
        }
        block_count++;
    }

    if (header_size + block_count * block_size != coded_stream->ByteCount()) {
        return false;
    }

    return true;
}

}  // namespace fpindex
