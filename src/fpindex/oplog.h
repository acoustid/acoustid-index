#pragma once

#include <mutex>

#include "fpindex/io/sqlite.h"
#include "fpindex/proto/internal.pb.h"

namespace fpindex {

class Oplog {
 public:
    Oplog(std::shared_ptr<sqlite3> db);
    bool Open();
    bool Write(const OplogEntries &entries);

 protected:
    bool CreateTable();

 private:
 std::mutex mutex_;
    std::shared_ptr<sqlite3> db_;
};

}  // namespace fpindex
