#include "fpindex/oplog.h"
#include "fpindex/logging.h"
#include "fpindex/util/cleanup.h"

#include <sqlite3.h>

namespace fpindex {

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

bool Oplog::Write(const OplogEntries &entries) {
    std::lock_guard<std::mutex> lock(mutex_);

    sqlite3_stmt *stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_.get(), "INSERT INTO oplog (op_id, op_data) VALUES (?, ?)", -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        LOG_ERROR() << "failed to prepare statement: " << sqlite3_errstr(rc);
        return false;
    }
    auto finalize_stmt = util::MakeCleanup([stmt]() { sqlite3_finalize(stmt); });

    char *err_msg = nullptr;
    rc = sqlite3_exec(db_.get(), "BEGIN TRANSACTION", nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
        LOG_ERROR() << "failed to begin transaction: " << err_msg;
        sqlite3_free(err_msg);
        return false;
    }

    bool rollback = false;
    std::string op_data;
    for (const auto &entry : entries.entries()) {
        if (!entry.SerializeToString(&op_data)) {
            LOG_ERROR() << "failed to serialize oplog entry";
            rollback = true;
            break;
        }
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
        sqlite3_bind_int(stmt, 1, entry.op_id());
        sqlite3_bind_blob(stmt, 2, op_data.data(), op_data.size(), SQLITE_STATIC);
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            LOG_ERROR() << "failed to insert oplog entry: " << sqlite3_errstr(rc);
            rollback = true;
            break;
        }
    }

    const char *end_txn_sql = rollback ? "ROLLBACK TRANSACTION" : "COMMIT TRANSACTION";
    rc = sqlite3_exec(db_.get(), end_txn_sql, nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
        LOG_ERROR() << "failed to " << (rollback ? "rollback" : "commit") << " transaction: " << err_msg;
        sqlite3_free(err_msg);
        return false;
    }

    return true;
};

}  // namespace fpindex