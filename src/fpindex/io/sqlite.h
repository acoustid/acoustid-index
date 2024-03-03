#pragma once

#include <memory>
#include <string>

typedef struct sqlite3 sqlite3;

namespace fpindex {
namespace io {

std::shared_ptr<sqlite3> OpenDatabase(const std::string &path, bool create = false);
bool CloseDatabase(std::shared_ptr<sqlite3> &db);

}  // namespace io
}  // namespace fpindex
