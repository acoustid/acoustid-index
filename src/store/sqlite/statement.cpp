#include "store/sqlite/database.h"

#include <QFile>

#include <sqlite3.h>

namespace Acoustid {

SQLiteStatement::SQLiteStatement(sqlite3_stmt *stmt)
    : m_stmt(stmt, &sqlite3_finalize)
{
}

void SQLiteStatement::exec()
{
    int rc = sqlite3_step(m_stmt.get());
    if (rc != SQLITE_DONE) {
        throw SQLiteException(sqlite3_errstr(rc));
    }
}

void SQLiteStatement::bindNull(int index)
{
    int rc = sqlite3_bind_null(m_stmt.get(), index);
    if (rc != SQLITE_OK) {
        throw SQLiteException(sqlite3_errstr(rc));
    }
}

void SQLiteStatement::bindInt(int index, int value)
{
    int rc = sqlite3_bind_int(m_stmt.get(), index, value);
    if (rc != SQLITE_OK) {
        throw SQLiteException(sqlite3_errstr(rc));
    }
}

void SQLiteStatement::bindBlob(int index, const QByteArray &value)
{
    int rc = sqlite3_bind_blob(m_stmt.get(), index, value.constData(), value.size(), SQLITE_TRANSIENT);
    if (rc != SQLITE_OK) {
        throw SQLiteException(sqlite3_errstr(rc));
    }
}

void SQLiteStatement::bindText(int index, const QString &value)
{
    int rc = sqlite3_bind_text(m_stmt.get(), index, value.toUtf8().constData(), -1, SQLITE_TRANSIENT);
    if (rc != SQLITE_OK) {
        throw SQLiteException(sqlite3_errstr(rc));
    }
}

int64_t SQLiteStatement::lastInsertRowId()
{
    return sqlite3_last_insert_rowid(sqlite3_db_handle(m_stmt.get()));
}

} // namespace Acoustid

