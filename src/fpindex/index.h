#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>

#include "fpindex/io/directory.h"
#include "fpindex/proto/internal.pb.h"
#include "fpindex/search_result.h"
#include "fpindex/update.h"

namespace fpindex {

class Segment;
class BaseSegment;
class SegmentBuilder;
class Oplog;

class IndexSnapshot {
 public:
    IndexSnapshot(std::shared_ptr<IndexInfo> info, std::map<uint32_t, std::shared_ptr<BaseSegment>> segments);

    bool Search(const std::vector<uint32_t> &hashes, std::vector<SearchResult> *results);

 private:
    std::shared_ptr<IndexInfo> info_;
    std::map<uint32_t, std::shared_ptr<BaseSegment>> segments_;
};

class Index {
 public:
    Index(std::shared_ptr<io::Directory> dir);

    bool Open();
    bool Close();
    bool IsReady();

    bool Update(IndexUpdate &&update);
    bool Search(const std::vector<uint32_t> &hashes, std::vector<SearchResult> *results);

 private:
    std::shared_ptr<SegmentBuilder> GetCurrentSegment();
    void Writer();

    void AddSegment(std::shared_ptr<BaseSegment> segment);
    int GetNextSegmentId();

    std::shared_ptr<Segment> LoadSegment(uint32_t id);

    std::mutex mutex_;
    std::atomic<bool> stop_{false};

    std::mutex writer_mutex_;
    std::condition_variable writer_cv_;
    std::unique_ptr<std::thread> writer_thread_;

    std::shared_ptr<IndexInfo> info_;
    std::map<uint32_t, std::shared_ptr<BaseSegment>> segments_;

    std::deque<std::pair<std::shared_ptr<IndexInfo>, std::shared_ptr<SegmentBuilder>>> segments_to_write_;
    std::shared_ptr<SegmentBuilder> current_segment_;

    std::atomic<std::shared_ptr<IndexSnapshot>> snapshot_;

    std::shared_ptr<io::Directory> dir_;
    std::shared_ptr<io::Database> db_;
    std::shared_ptr<Oplog> oplog_;
};

}  // namespace fpindex
