#include <sqlite3.h>

#include "store/sqlite/error.h"

namespace Acoustid {

SQLiteError::SQLiteError(int rc)
    : std::runtime_error(sqlite3_errstr(rc)), m_rc(rc) {}

} // namespace Acoustid
