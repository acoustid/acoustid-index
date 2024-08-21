#include "fpindex/index.h"

#include <unordered_map>

#include "fpindex/logging.h"
#include "fpindex/oplog.h"
#include "fpindex/proto/internal.pb.h"
#include "fpindex/segment.h"
#include "fpindex/segment_builder.h"

namespace fpindex {

constexpr size_t MAX_STAGE_SIZE = 1000000;

std::string GenerateSegmentFileName(uint32_t id) { return "segment-" + std::to_string(id) + ".data"; }

IndexSnapshot::IndexSnapshot(std::shared_ptr<IndexInfo> info, std::map<uint32_t, std::shared_ptr<BaseSegment>> segments)
    : info_(info), segments_(segments) {}

bool IndexSnapshot::Search(const std::vector<uint32_t>& hashes, std::vector<SearchResult>* results) { return false; }

void Index::Writer() {
    while (true) {
        std::unique_lock<std::mutex> writer_lock(writer_mutex_);
        writer_cv_.wait_for(writer_lock, std::chrono::seconds(1));

        if (stop_) {
            return;
        }

        std::unique_lock<std::mutex> lock(mutex_);

        if (segments_to_write_.empty()) {
            continue;
        }
        auto to_write = segments_to_write_.front();

        lock.unlock();

        auto index_info = to_write.first;
        auto segment_builder = to_write.second;

        auto file_name = GenerateSegmentFileName(segment_builder->id());
        auto file = dir_->OpenFile(file_name, true);
        if (!file) {
            LOG_ERROR() << "failed to open file " << QString::fromStdString(file_name);
            continue;
        }

        auto segment = segment_builder->Save(file);
        if (!segment) {
            LOG_ERROR() << "failed to write segment";
            continue;
        }

        lock.lock();
        segments_.insert({segment->id(), segment});
        segments_to_write_.pop_front();
    }
}

Index::Index(std::shared_ptr<io::Directory> dir) : dir_(dir) {}

bool Index::Open() {
    std::lock_guard<std::mutex> lock(mutex_);

    writer_thread_ = std::make_unique<std::thread>(&Index::Writer, this);

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

    info_ = std::make_shared<IndexInfo>();
    for (auto& segment_info : info_->segments()) {
        auto segment = LoadSegment(segment_info.id());
        if (!segment) {
            LOG_ERROR() << "failed to load segment" << segment_info.id();
            return false;
        }
        segments_.insert({segment->id(), segment});
    }
    snapshot_ = std::make_shared<IndexSnapshot>(info_, segments_);

    return true;
}

std::shared_ptr<Segment> Index::LoadSegment(uint32_t id) {
    auto file_name = GenerateSegmentFileName(id);
    auto file = dir_->OpenFile(file_name, false);
    if (!file) {
        LOG_ERROR() << "failed to open file" << QString::fromStdString(file_name);
        return nullptr;
    }
    auto segment = std::make_shared<Segment>(id);
    if (!segment->Load(file)) {
        LOG_ERROR() << "failed to load segment" << QString::fromStdString(file_name);
        return nullptr;
    }
    return segment;
}

bool Index::Close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (oplog_) {
        oplog_->Close();
        oplog_.reset();
    }

    stop_ = true;
    writer_cv_.notify_all();

    writer_thread_->join();
    writer_thread_.reset();
    return true;
}

bool Index::IsReady() { return oplog_ && oplog_->IsReady() && writer_thread_; }

bool Index::Search(const std::vector<uint32_t>& hashes, std::vector<SearchResult>* results) {
    auto snapshot = snapshot_.load();
    return snapshot->Search(hashes, results);
}

void Index::AddSegment(std::shared_ptr<BaseSegment> segment) {
    segments_.insert({segment->id(), segment});
    for (int i = info_->segments_size() - 1; i >= 0; i--) {
        if (info_->segments(i).id() == segment->id()) {
            info_->mutable_segments(i)->CopyFrom(segment->info());
            return;
        }
    }
    info_->add_segments()->CopyFrom(current_segment_->info());
    snapshot_.store(std::make_shared<IndexSnapshot>(info_, segments_));
}

int Index::GetNextSegmentId() {
    for (auto i = info_->segments_size() - 1; i >= 0; i--) {
        return info_->segments(i).id() + 1;
    }
    return 0;
}

bool Index::Update(IndexUpdate&& update) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!oplog_ || !oplog_->IsReady()) {
        LOG_ERROR() << "oplog is not open";
        return false;
    }

    auto last_oplog_id = oplog_->GetLastId();
    if (!last_oplog_id) {
        LOG_ERROR() << "failed to get last oplog id";
        return false;
    }

    if (!current_segment_) {
        current_segment_ = std::make_shared<SegmentBuilder>(GetNextSegmentId());
        AddSegment(current_segment_);
    }

    if (current_segment_->Size() >= MAX_STAGE_SIZE) {
        auto next_segment = std::make_shared<SegmentBuilder>(GetNextSegmentId());
        auto next_index_info = std::make_shared<IndexInfo>(*info_);
        segments_to_write_.push_back({info_, current_segment_});
        current_segment_ = next_segment;
        info_ = next_index_info;
        AddSegment(current_segment_);
        writer_cv_.notify_one();
    }

    if (current_segment_->max_oplog_id() != last_oplog_id) {
        LOG_ERROR() << "current_segment is not up-to-date";
        return false;
    }

    auto check_update = [this](const auto& entries) {
        return current_segment_->CheckUpdate(entries);
    };
    auto entries = update.Finish();
    if (!oplog_->Write(entries, check_update)) {
        LOG_ERROR() << "failed to write oplog";
        return false;
    }

    current_segment_->Update(entries);

    for (auto i = info_->segments_size() - 1; i >= 0; i--) {
        if (info_->segments(i).id() == current_segment_->id()) {
            info_->mutable_segments(i)->CopyFrom(current_segment_->info());
            break;
        }
    }

    for (const auto& entry : entries) {
        if (entry.data().has_set_attribute()) {
            auto data = entry.data().set_attribute();
            info_->mutable_attributes()->insert({data.name(), data.value()});
        }
    }

    snapshot_.store(std::make_shared<IndexSnapshot>(info_, segments_));

    return true;
}

}  // namespace fpindex
