#pragma once

#include <memory>

#include <QString>

class sqlite3_stmt;

namespace Acoustid {

class SQLiteStatement
{
 public:
    SQLiteStatement(sqlite3_stmt *stmt);

    SQLiteStatement(const SQLiteStatement &other) = delete;
    SQLiteStatement &operator=(const SQLiteStatement &other) = delete;

    SQLiteStatement(SQLiteStatement &&other) = default;
    SQLiteStatement &operator=(SQLiteStatement &&other) = default;

    void bindNull(int index);
    void bindInt(int index, int value);
    void bindBlob(int index, const QByteArray &value);
    void bindText(int index, const QString &value);

    sqlite3_stmt *handle() const { return m_stmt.get(); }

    void exec();

    int64_t lastInsertRowId();

 private:
    std::unique_ptr<sqlite3_stmt, int(*)(sqlite3_stmt *)> m_stmt;
};

}  // namespace Acoustid