#pragma once

#include <map>
#include <mutex>
#include <variant>

#include "fpindex/io/directory.h"
#include "fpindex/io/memory_file.h"

namespace fpindex {
namespace io {

class MemoryDirectory : public Directory {
 public:
    std::shared_ptr<File> OpenFile(const std::string& name, bool create = false) override;
    std::shared_ptr<Directory> OpenDirectory(const std::string& name, bool create = false) override;
    std::vector<std::string> ListFiles() override;
    std::vector<std::string> ListDirectories() override;

 private:
    std::mutex mutex_;
    std::map<std::string, std::variant<std::shared_ptr<MemoryFile>, std::shared_ptr<MemoryDirectory>>> entries_;
};

}  // namespace io
}  // namespace fpindex
