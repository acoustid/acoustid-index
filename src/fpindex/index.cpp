#include "fpindex/index.h"

#include <atomic>
#include <unordered_map>

#include "fpindex/logging.h"
#include "fpindex/oplog.h"
#include "fpindex/proto/internal.pb.h"
#include "fpindex/segment.h"
#include "fpindex/segment_builder.h"

namespace fpindex {

class SegmentCache {
 public:
    std::shared_ptr<BaseSegment> GetSegment(uint32_t id);
    void AddSegment(uint32_t id, std::shared_ptr<BaseSegment> segment);

 private:
    std::mutex mutex_;
    std::shared_ptr<std::unordered_map<uint32_t, std::weak_ptr<BaseSegment>>> segments_;
};

std::shared_ptr<BaseSegment> SegmentCache::GetSegment(uint32_t id) {
    auto segments = segments_;
    if (!segments) {
        return nullptr;
    }
    auto it = segments->find(id);
    if (it != segments->end()) {
        auto segment = it->second.lock();
        if (segment) {
            return segment;
        }
    }
    return nullptr;
}

void SegmentCache::AddSegment(uint32_t id, std::shared_ptr<BaseSegment> segment) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto new_segments = std::make_shared<std::unordered_map<uint32_t, std::weak_ptr<BaseSegment>>>();
    auto old_segments = segments_;
    if (old_segments) {
        for (auto& it : *old_segments) {
            if (!it.second.expired()) {
                new_segments->insert(it);
            }
        }
    }
    new_segments->insert({id, segment});
    segments_ = new_segments;
}

class IndexData {
 public:
    IndexData(std::shared_ptr<SegmentCache> segment_cache, std::shared_ptr<IndexInfo> info);

    bool IsReady();
    bool Open();

    bool Search(const std::vector<uint32_t>& hashes, std::vector<SearchResult>* results);

 private:
    friend class Index;

    std::shared_ptr<SegmentCache> segment_cache_;
    std::shared_ptr<SegmentBuilder> stage_;
    std::shared_ptr<IndexInfo> info_;
};

IndexData::IndexData(std::shared_ptr<SegmentCache> segment_cache, std::shared_ptr<IndexInfo> info)
    : segment_cache_(segment_cache), info_(info) {
    uint32_t next_segment_id = 0;
    if (info_->segments_size() > 0) {
        next_segment_id = info_->segments(info_->segments_size() - 1).id() + 1;
    }

    stage_ = std::make_shared<SegmentBuilder>(next_segment_id);
}

bool IndexData::IsReady() {
    for (auto& segment_info : info_->segments()) {
        auto segment = segment_cache_->GetSegment(segment_info.id());
        if (!segment) {
            return false;
        }
        if (!segment->IsReady()) {
            return false;
        }
    }
    return true;
}

bool IndexData::Search(const std::vector<uint32_t>& hashes, std::vector<SearchResult>* results) {
    std::vector<std::shared_ptr<BaseSegment>> segments;
    segments.reserve(info_->segments_size() + 1);
    for (auto& segment_info : info_->segments()) {
        auto segment = segment_cache_->GetSegment(segment_info.id());
        if (!segment) {
            return false;
        }
        if (!segment->IsReady()) {
            return false;
        }
        segments.push_back(segment);
    }
    segments.push_back(stage_);

    results->clear();
    std::vector<SearchResult> partial_results;
    for (auto& segment : segments) {
        if (!segment->Search(hashes, &partial_results)) {
            return false;
        }
        results->insert(results->end(), partial_results.begin(), partial_results.end());
    }
    return true;
}

Index::Index(std::shared_ptr<io::Directory> dir) : dir_(dir) {}

bool Index::Open() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (data_) {
        return true;
    }

    db_ = dir_->OpenDatabase("index.db", true);
    if (!db_) {
        LOG_ERROR() << "failed to open database";
        return false;
    }

    oplog_ = std::make_shared<Oplog>(db_);
    if (!oplog_->Open()) {
        LOG_ERROR() << "failed to open oplog";
        return false;
    }

    auto index_info = std::make_shared<IndexInfo>();
    for (auto& segment_info : index_info->segments()) {
        auto segment = std::make_shared<Segment>(segment_info.id());
        if (!segment->Load(nullptr)) {
            return false;
        }
        segment_cache_->AddSegment(segment_info.id(), segment);
    }

    data_ = std::make_shared<IndexData>(segment_cache_, index_info);
    return true;
}

bool Index::Close() {
    std::lock_guard<std::mutex> lock(mutex_);
    return true;
}

bool Index::IsReady() {
    auto data = data_;
    return data && data->IsReady();
}

bool Index::Search(const std::vector<uint32_t>& hashes, std::vector<SearchResult>* results) {
    auto data = data_;
    if (!data) {
        return false;
    }
    return data->Search(hashes, results);
}

bool Index::Update(IndexUpdate&& update) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!oplog_ || !oplog_->IsReady()) {
        LOG_ERROR() << "oplog is not open";
        return false;
    }

    auto entries = update.Finish();
    if (!oplog_->Write(entries)) {
        LOG_ERROR() << "failed to write to oplog";
        return false;
    }

    return true;
}

}  // namespace fpindex
