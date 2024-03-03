#pragma once

#include "fpindex/io/sqlite.h"

namespace fpindex {

class Oplog {
 public:
    Oplog(std::shared_ptr<sqlite3> db);

 protected:
    bool CreateTable();

 private:
    std::shared_ptr<sqlite3> db_;
};

}  // namespace fpindex
