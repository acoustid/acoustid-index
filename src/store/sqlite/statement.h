#ifndef ACOUSTID_INDEX_STORE_SQL_STATEMENT_H_
#define ACOUSTID_INDEX_STORE_SQL_STATEMENT_H_

#include <QString>
#include <memory>

#include "util/exceptions.h"

class sqlite3_stmt;

namespace Acoustid {

class SQLiteStatement {
 public:
    SQLiteStatement(sqlite3_stmt *stmt);

    SQLiteStatement(const SQLiteStatement &other) = delete;
    SQLiteStatement &operator=(const SQLiteStatement &other) = delete;

    void bindNull(int index);
    void bindInt(int index, int value);
    void bindBlob(int index, const QByteArray &value);
    void bindText(int index, const QString &value);

    void exec();

    int64_t lastInsertRowId();

 private:
    std::unique_ptr<sqlite3_stmt, int (*)(sqlite3_stmt *)> m_stmt;
};

}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_STORE_SQL_STATEMENT_H_
