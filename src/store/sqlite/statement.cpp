#include <sqlite3.h>

#include <QFile>
#include <QDebug>

#include "store/sqlite/database.h"

namespace Acoustid {

static void finalizeSqlite3Statement(sqlite3_stmt *stmt) {
    auto rc = sqlite3_finalize(stmt);
    if (rc != SQLITE_OK) {
        qWarning() << "sqlite3_finalize failed:" << sqlite3_errstr(rc);
    }
}

SQLiteStatement::SQLiteStatement(const std::shared_ptr<sqlite3> &db, sqlite3_stmt *stmt)
    : m_db(db), m_stmt(stmt, &finalizeSqlite3Statement) {}

SQLiteResult SQLiteStatement::exec() {
    int rc = sqlite3_step(m_stmt.get());
    if (rc != SQLITE_DONE) {
        throw SQLiteException(sqlite3_errstr(rc));
    }
    return SQLiteResult(m_db);
}

void SQLiteStatement::bindNull(int index) {
    int rc = sqlite3_bind_null(m_stmt.get(), index);
    if (rc != SQLITE_OK) {
        throw SQLiteException(sqlite3_errstr(rc));
    }
}

void SQLiteStatement::bindInt(int index, int value) {
    int rc = sqlite3_bind_int(m_stmt.get(), index, value);
    if (rc != SQLITE_OK) {
        throw SQLiteException(sqlite3_errstr(rc));
    }
}

void SQLiteStatement::bindBlob(int index, const QByteArray &value) {
    int rc = sqlite3_bind_blob(m_stmt.get(), index, value.constData(), value.size(), SQLITE_TRANSIENT);
    if (rc != SQLITE_OK) {
        throw SQLiteException(sqlite3_errstr(rc));
    }
}

void SQLiteStatement::bindText(int index, const QString &value) {
    int rc = sqlite3_bind_text(m_stmt.get(), index, value.toUtf8().constData(), -1, SQLITE_TRANSIENT);
    if (rc != SQLITE_OK) {
        throw SQLiteException(sqlite3_errstr(rc));
    }
}

}  // namespace Acoustid
