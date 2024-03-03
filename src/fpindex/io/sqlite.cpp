#include "fpindex/io/sqlite.h"

#include <sqlite3.h>

#include "fpindex/logging.h"

namespace fpindex {
namespace io {

static int sqlite3_close_and_log_error(sqlite3* db) {
    int rc = sqlite3_close(db);
    if (rc) {
        LOG_ERROR() << "Can't close SQLite database: " << sqlite3_errstr(rc);
    }
    return rc;
}

class SQLite3Deleter {
 public:
    void operator()(sqlite3* db) const {
        if (cancelled_) {
            return;
        }
        LOG_WARNING() << "Closing SQLite database in destructor";
        sqlite3_close_and_log_error(db);
    }

    void Cancel() { cancelled_ = true; }

    bool cancelled_{false};
};

std::shared_ptr<sqlite3> OpenDatabase(const std::string& db_name, bool create) {
    sqlite3* db = nullptr;
    int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX;
    if (create) {
        flags |= SQLITE_OPEN_CREATE;
    }
    int rc = sqlite3_open_v2(db_name.c_str(), &db, flags, nullptr);
    if (rc) {
        LOG_ERROR() << "Can't open SQLite database: " << sqlite3_errstr(rc);
        sqlite3_close_and_log_error(db);
        return nullptr;
    }
    return std::shared_ptr<sqlite3>(db, SQLite3Deleter());
}

bool CloseDatabase(std::shared_ptr<sqlite3>& db) {
    if (!db) {
        return true;
    }
    if (sqlite3_close_and_log_error(db.get())) {
        return false;
    }
    if (auto deleter = std::get_deleter<SQLite3Deleter>(db); deleter) {
        deleter->Cancel();
    }
    db.reset();
    return true;
}

}  // namespace io
}  // namespace fpindex
