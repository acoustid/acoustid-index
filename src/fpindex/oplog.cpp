#include "fpindex/oplog.h"

#include <sqlite3.h>

#include "fpindex/logging.h"
#include "fpindex/util/cleanup.h"

namespace fpindex {

Oplog::Oplog(std::shared_ptr<io::Database> db) : db_(db) {}

static const char *kCreateOplogTableSql = R"(
    CREATE TABLE IF NOT EXISTS oplog (
        op_id INTEGER PRIMARY KEY AUTOINCREMENT,
        op_data BLOB NOT NULL
    );
)";

static const char *kInsertOplogEntrySql = R"(
    INSERT INTO oplog (op_data) VALUES (?) RETURNING op_id;
)";

bool Oplog::CreateTable() {
    char *err_msg = nullptr;
    int rc = sqlite3_exec(db_->get(), kCreateOplogTableSql, nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
        LOG_ERROR() << "failed to create oplog error: " << err_msg;
        sqlite3_free(err_msg);
        return false;
    }
    return true;
}

bool Oplog::IsReady() {
    std::lock_guard<std::mutex> lock(mutex_);
    return ready_;
}

bool Oplog::Open() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!CreateTable()) {
        return false;
    }
    ready_ = true;
    return true;
}

bool Oplog::Write(std::vector<OplogEntry> &entries) {
    std::lock_guard<std::mutex> lock(mutex_);

    sqlite3 *db = db_->get();

    sqlite3_stmt *stmt = nullptr;
    int rc = sqlite3_prepare_v2(db, kInsertOplogEntrySql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        LOG_ERROR() << "failed to prepare statement: " << sqlite3_errstr(rc);
        return false;
    }
    auto finalize_stmt = util::MakeCleanup([stmt]() {
        sqlite3_finalize(stmt);
    });

    char *err_msg = nullptr;
    rc = sqlite3_exec(db, "BEGIN TRANSACTION", nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
        LOG_ERROR() << "failed to begin transaction: " << err_msg;
        sqlite3_free(err_msg);
        return false;
    }

    bool rollback = false;
    std::string op_data;
    for (auto &entry : entries) {
        if (!entry.data().SerializeToString(&op_data)) {
            LOG_ERROR() << "failed to serialize oplog entry";
            rollback = true;
            break;
        }
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
        sqlite3_bind_blob(stmt, 1, op_data.data(), op_data.size(), SQLITE_STATIC);
        while (true) {
            rc = sqlite3_step(stmt);
            if (rc == SQLITE_ROW) {
                auto op_id = sqlite3_column_int64(stmt, 0);
                LOG_INFO() << "inserted oplog entry with id: " << op_id;
                entry.set_id(op_id);
                continue;
            } else if (rc == SQLITE_DONE) {
                break;
            } else {
                LOG_ERROR() << "failed to insert oplog entry: " << sqlite3_errstr(rc);
                rollback = true;
                break;
            }
        }
    }

    const char *end_txn_sql = rollback ? "ROLLBACK TRANSACTION" : "COMMIT TRANSACTION";
    rc = sqlite3_exec(db, end_txn_sql, nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
        LOG_ERROR() << "failed to " << (rollback ? "rollback" : "commit") << " transaction: " << err_msg;
        sqlite3_free(err_msg);
        return false;
    }

    return true;
};

}  // namespace fpindex
