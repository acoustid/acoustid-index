#include "fpindex/io/memory_directory.h"

namespace fpindex {
namespace io {

std::shared_ptr<File> MemoryDirectory::OpenFile(const std::string &name, bool create) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto iter = files_.find(name);
  if (iter != files_.end()) {
    return iter->second;
  }
  if (!create) {
    return nullptr;
  }
  auto file = std::make_shared<MemoryFile>();
  files_[name] = file;
  return file;
}

}
}
