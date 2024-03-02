#pragma once

#include <map>
#include <mutex>

#include "fpindex/io/directory.h"
#include "fpindex/io/memory_file.h"

namespace fpindex {
namespace io {

class MemoryDirectory : public Directory {
 public:
    std::shared_ptr<File> OpenFile(const std::string& name, bool create = false) override;

 private:
    std::mutex mutex_;
    std::map<std::string, std::shared_ptr<MemoryFile>> files_;
};

}  // namespace io
}  // namespace fpindex
