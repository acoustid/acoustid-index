#ifndef ACOUSTID_INDEX_STORE_SQL_DATABASE_H_
#define ACOUSTID_INDEX_STORE_SQL_DATABASE_H_

#include <QString>

#include <memory>

#include "util/exceptions.h"
#include "store/sqlite/statement.h"

class sqlite3;

namespace Acoustid {

class SQLiteException : public Exception {
 public:
    SQLiteException(const QString &msg) : Exception(msg) {}
};

class SQLiteDatabase
{
 public:
    SQLiteDatabase(const QString &name);
    ~SQLiteDatabase();

    SQLiteStatement prepare(const QString &sql);

 private:
    std::shared_ptr<sqlite3> m_db;
};

}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_STORE_SQL_DATABASE_H_
