#include "fpindex/oplog.h"

#include <sqlite3.h>

Oplog::Oplog(std::shared_ptr<sqlite3> db) : db_(db) {}

static const char *kCreateOplogTableSql = R"(
    CREATE TABLE IF NOT EXISTS oplog (
        op_id INTEGER PRIMARY KEY,
        op_data BLOB NOT NULL
    ) WITHOUT ROWID;
)";

bool Oplog::CreateTable() {
    char *err_msg = nullptr;
    int rc = sqlite3_exec(db_.get(), kCreateOplogTableSql, nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
        LOG_ERROR() << "failed to create oplog error: " << err_msg;
        sqlite3_free(err_msg);
        return false;
    }
    return true;
}

bool Oplog::Open() {
    if (!CreateTable()) {
        return false;
    }
    return true;
}