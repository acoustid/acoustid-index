#pragma once

#include "fpindex/io/sqlite.h"

namespace fpindex {
namespace io {

class Database {
public:
        explicit Database(std::shared_ptr<sqlite3> db) : db_(db) {}
        ~Database() { Close(); }

        bool Close() { return CloseDatabase(db_); }

        sqlite3 *get() const { return db_.get(); }
        sqlite3* operator->() const { return db_.get(); }
        operator bool() const { return bool(db_); }

private:
        std::shared_ptr<sqlite3> db_;


};

}  // namespace io
}  // namespace fpindex
