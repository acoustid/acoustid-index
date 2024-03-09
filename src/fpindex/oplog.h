#pragma once

#include <mutex>

#include "fpindex/io/database.h"
#include "fpindex/proto/internal.pb.h"

namespace fpindex {

class Oplog {
 public:
    Oplog(std::shared_ptr<io::Database> db);
    bool Open();
    bool Write(std::vector<OplogEntry> &entries);
    bool IsReady();

 protected:
    bool CreateTable();

 private:
    std::mutex mutex_;
    std::shared_ptr<io::Database> db_;
    bool ready_{false};
};

}  // namespace fpindex
