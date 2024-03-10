#pragma once

#include <mutex>
#include <optional>

#include "fpindex/io/database.h"
#include "fpindex/proto/internal.pb.h"

namespace fpindex {

class Oplog {
 public:
    Oplog(std::shared_ptr<io::Database> db);
    bool Open();
    bool Close();
    bool Write(std::vector<OplogEntry> &entries, std::function<bool(std::vector<OplogEntry> &)> callback = nullptr);
    bool IsReady();
    std::optional<uint64_t> GetLastId();

 protected:
    bool CreateTable();

 private:
    std::mutex mutex_;
    std::shared_ptr<io::Database> db_;
    bool ready_{false};
};

}  // namespace fpindex
