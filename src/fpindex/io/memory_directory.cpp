#include "fpindex/logging.h"
#include "fpindex/io/memory_directory.h"

namespace fpindex {
namespace io {

std::shared_ptr<File> MemoryDirectory::OpenFile(const std::string &name, bool create) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto iter = entries_.find(name);
    if (iter != entries_.end()) {
        auto entry = iter->second;
        try {
            return std::get<std::shared_ptr<MemoryFile>>(entry);
        } catch (std::bad_variant_access &e) {
            return nullptr;
        }
    }
    if (!create) {
        return nullptr;
    }
    auto file = std::make_shared<MemoryFile>();
    entries_[name] = file;
    return file;
}

std::shared_ptr<Directory> MemoryDirectory::OpenDirectory(const std::string &name, bool create) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto iter = entries_.find(name);
    if (iter != entries_.end()) {
        auto entry = iter->second;
        try {
            return std::get<std::shared_ptr<MemoryDirectory>>(entry);
        } catch (std::bad_variant_access &e) {
            return nullptr;
        }
    }
    if (!create) {
        return nullptr;
    }
    auto directory = std::make_shared<MemoryDirectory>();
    entries_[name] = directory;
    return directory;
}

std::shared_ptr<io::Database> MemoryDirectory::OpenDatabase(const std::string &name, bool create) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto iter = entries_.find(name);
    if (iter != entries_.end()) {
        auto entry = iter->second;
        try {
            return std::get<std::shared_ptr<io::Database>>(entry);
        } catch (std::bad_variant_access &e) {
            return nullptr;
        }
    }
    if (!create) {
        return nullptr;
    }
    auto db = std::make_shared<io::Database>(io::OpenDatabase(":memory:", true));
    entries_[name] = db;
    return db;
}

std::vector<std::string> MemoryDirectory::ListFiles() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> files;
    for (auto &entry : entries_) {
        if (std::holds_alternative<std::shared_ptr<MemoryFile>>(entry.second)) {
            files.push_back(entry.first);
        }
    }
    return files;
}

std::vector<std::string> MemoryDirectory::ListDirectories() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> directories;
    for (auto &entry : entries_) {
        if (std::holds_alternative<std::shared_ptr<MemoryDirectory>>(entry.second)) {
            directories.push_back(entry.first);
        }
    }
    return directories;
}

}  // namespace io
}  // namespace fpindex
