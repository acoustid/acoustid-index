#pragma once

#include <memory>
#include <string>

#include <sqlite3.h>

namespace fpindex {
namespace io {

std::shared_ptr<sqlite3> OpenDatabase(const std::string &path, bool create = false);
bool CloseDatabase(std::shared_ptr<sqlite3> &db);

}  // namespace io
}  // namespace fpindex
