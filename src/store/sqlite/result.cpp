#include "store/sqlite/result.h"

#include <sqlite3.h>

#include "store/sqlite/database.h"

namespace Acoustid {

SQLiteResult::SQLiteResult(const std::shared_ptr<sqlite3> &db) : m_db(db) {}

int64_t SQLiteResult::lastInsertId() { return sqlite3_last_insert_rowid(m_db.get()); }

int64_t SQLiteResult::rowsAffected() { return sqlite3_changes(m_db.get()); }

}  // namespace Acoustid
