#pragma once

#include <stdexcept>

namespace Acoustid {

class SQLiteError : public std::runtime_error {
 public:
    explicit SQLiteError(int rc);

    int code() const { return m_rc; }

 private:
    int m_rc;
};

} // namespace Acoustid
