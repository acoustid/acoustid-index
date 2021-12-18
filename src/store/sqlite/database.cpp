#include "store/sqlite/database.h"

#include <QFile>

#include <sqlite3.h>

namespace Acoustid {

SQLiteDatabase::SQLiteDatabase(const QString &fileName)
{
    sqlite3 *db;
    auto encodedFileName = QFile::encodeName(fileName);
    auto encodedFileNamePtr = encodedFileName.data();
    int rc = sqlite3_open(encodedFileNamePtr, &db);
    if (rc != SQLITE_OK) {
        throw SQLiteException(sqlite3_errstr(rc));
    }
    m_db = std::shared_ptr<sqlite3>(db, sqlite3_close_v2);
}

SQLiteStatement SQLiteDatabase::prepare(const QString &query)
{
    sqlite3_stmt *stmt;
    auto encodedQuery = QByteArray(query.toUtf8());
    auto encodedQueryPtr = encodedQuery.data();
    int rc = sqlite3_prepare_v2(m_db.get(), encodedQueryPtr, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        throw SQLiteException(sqlite3_errstr(rc));
    }
    return SQLiteStatement(stmt);
}

} // namespace Acoustid
