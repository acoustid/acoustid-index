#ifndef ACOUSTID_INDEX_STORE_SQL_RESULT_H_
#define ACOUSTID_INDEX_STORE_SQL_RESULT_H_

#include <memory>

struct sqlite3;

namespace Acoustid {

class SQLiteResult {
 public:
    SQLiteResult(const std::shared_ptr<sqlite3> &db);

    int64_t lastInsertId();
    int64_t rowsAffected();

 private:
    std::shared_ptr<sqlite3> m_db;
};

}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_STORE_SQL_RESULT_H_
