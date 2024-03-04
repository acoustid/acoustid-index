#include "fpindex/segment_builder.h"

#include <google/protobuf/io/coded_stream.h>

#include <mutex>

#include "fpindex/search_result.h"
#include "fpindex/logging.h"
#include "fpindex/segment.h"
#include "fpindex/segment_file_format.h"

namespace fpindex {

bool SegmentBuilder::InsertOrUpdate(uint32_t id, const std::vector<uint32_t>& hashes) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (frozen_) {
        return false;
    }
    DeleteInternal(id);
    for (auto hash : hashes) {
        data_.insert(std::make_pair(hash, id));
    }
    ids_.insert(id);
    return true;
}

bool SegmentBuilder::Delete(uint32_t id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (frozen_) {
        return false;
    }
    DeleteInternal(id);
    return true;
}

void SegmentBuilder::DeleteInternal(uint32_t id) {
    if (auto it = ids_.find(id); it != ids_.end()) {
        ids_.erase(it);
        std::erase_if(data_, [id](const auto& pair) { return pair.second == id; });
    }
}

bool SegmentBuilder::Contains(uint32_t id) {
    std::shared_lock<std::shared_mutex> lock;
    if (!frozen_) {
        lock = std::shared_lock<std::shared_mutex>(mutex_);
    }
    return ids_.contains(id);
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
            scores[it->second]++;
        }
    }
    results->clear();
    results->reserve(scores.size());
    for (auto score : scores) {
        results->emplace_back(score.first, score.second);
    }
    return true;
}

std::shared_ptr<Segment> SegmentBuilder::Save(const std::shared_ptr<io::File>& file) {
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
        return nullptr;
    }

    const int block_size = header.block_size();
    const int header_size = coded_stream->ByteCount();

    int block_count = 0;
    auto it = data_.begin();
    std::vector<std::pair<uint32_t, uint32_t>> block_index;
    while (it != data_.end()) {
        auto next_it = internal::SerializeSegmentBlock(coded_stream.get(), header, it, data_.end());
        if (coded_stream->HadError()) {
            return nullptr;
        }
        auto first_key = it->first, last_key = std::prev(next_it)->first;
        block_index.emplace_back(first_key, last_key);
        it = next_it;
        block_count++;
    }

    if (header_size + block_count * block_size != coded_stream->ByteCount()) {
        return nullptr;
    }

    auto result = std::make_shared<Segment>(id());
    result->Load(file, header, header_size, std::move(block_index));
    return result;
}

}  // namespace fpindex
