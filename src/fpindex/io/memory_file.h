#pragma once

#include <string>

#include "fpindex/io/file.h"

namespace fpindex {
namespace io {

class MemoryFile : public File {
 public:
    MemoryFile();
    MemoryFile(const MemoryFile& other) = delete;
    void operator=(const MemoryFile& other) = delete;

    size_t Size() override;
    std::unique_ptr<ZeroCopyInputStream> GetInputStream() override;
    std::unique_ptr<ZeroCopyOutputStream> GetOutputStream() override;
    std::unique_ptr<CodedInputStream> GetCodedInputStream(size_t offset, size_t size) override;

 private:
    std::string data_;
};

}  // namespace io
}  // namespace fpindex
