#pragma once

#include <memory>
#include <mutex>

#include "fpindex/io/directory.h"
#include "fpindex/proto/internal.pb.h"
#include "fpindex/search_result.h"

namespace fpindex {

class IndexData;
class SegmentCache;
class Oplog;

class Index {
 public:
    Index(std::shared_ptr<io::Directory> dir);

    bool Open();
    bool IsReady();

    bool Update(const std::vector<OplogEntryData> &updates);
    bool Search(const std::vector<uint32_t> &hashes, std::vector<SearchResult> *results);

 private:
    std::mutex mutex_;
    std::shared_ptr<io::Directory> dir_;
    std::shared_ptr<SegmentCache> segment_cache_;
    std::shared_ptr<IndexData> data_;
    std::shared_ptr<Oplog> oplog_;
};

}  // namespace fpindex
