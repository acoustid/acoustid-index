#include "fpindex/in_memory_segment.h"

#include <google/protobuf/io/coded_stream.h>

#include <mutex>

#include "fpindex/search_result.h"
#include "fpindex/segment_file_format.h"

namespace fpindex {

bool InMemorySegment::Add(uint32_t id, const std::vector<uint32_t>& hashes) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (frozen_) {
        return false;
    }
    for (auto hash : hashes) {
        data_.insert(std::make_pair(id, hash));
    }
    return true;
}

void InMemorySegment::Freeze() {
    if (!frozen_) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        frozen_ = true;
    }
}

bool InMemorySegment::Search(const std::vector<uint32_t>& hashes, std::vector<SearchResult>* results) {
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

bool InMemorySegment::Serialize(io::ZeroCopyOutputStream* output) {
    io::CodedOutputStream coded_output(output);
    return Serialize(&coded_output);
}

bool InMemorySegment::Serialize(io::CodedOutputStream* output) {
    std::shared_lock<std::shared_mutex> lock;
    if (!frozen_) {
        lock = std::shared_lock<std::shared_mutex>(mutex_);
    }

    SegmentHeader header;
    internal::InitializeSegmentHeader(&header);
    internal::SerializeSegmentHeader(output, header);
    if (output->HadError()) {
        return false;
    }

    const int block_size = header.block_size();
    const int header_size = output->ByteCount();

    int block_count = 0;
    auto it = data_.begin();
    while (it != data_.end()) {
        it = internal::SerializeSegmentBlock(output, header, it, data_.end());
        if (output->HadError()) {
            return false;
        }
        block_count++;
    }

    if (header_size + block_count * block_size != output->ByteCount()) {
        return false;
    }

    return true;
}

}  // namespace fpindex
