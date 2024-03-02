#pragma once

#include <memory>
#include <vector>
#include <string>

#include "fpindex/io/file.h"

namespace fpindex {
namespace io {

class Directory {
 public:
    virtual std::shared_ptr<File> OpenFile(const std::string& name, bool create = false) = 0;
    virtual std::shared_ptr<Directory> OpenDirectory(const std::string& name, bool create = false) = 0;
    virtual std::vector<std::string> ListFiles() = 0;
    virtual std::vector<std::string> ListDirectories() = 0;
};

}  // namespace io
}  // namespace fpindex
