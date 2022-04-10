#pragma once

#include <string>
#include <memory>

#include <QString>

#include "store/sqlite/statement.h"

class sqlite3;

namespace Acoustid {

class SQLiteDatabase
{
 public:
    SQLiteDatabase(const QString &name);

    SQLiteDatabase(const SQLiteDatabase &other) = default;
    SQLiteDatabase &operator=(const SQLiteDatabase &other) = default;

    SQLiteDatabase(SQLiteDatabase &&other) = default;
    SQLiteDatabase &operator=(SQLiteDatabase &&other) = default;

    sqlite3 *handle() const { return m_db.get(); }

    SQLiteStatement prepare(const QString &sql);

 private:
    std::shared_ptr<sqlite3> m_db;
};

}  // namespace Acoustid
