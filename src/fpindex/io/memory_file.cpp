#include "fpindex/io/memory_file.h"

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

namespace fpindex {
namespace io {

MemoryFile::MemoryFile() {}

size_t MemoryFile::Size() {
  return data_.size();
}

std::unique_ptr<ZeroCopyInputStream> MemoryFile::GetInputStream() {
return std::make_unique<google::protobuf::io::ArrayInputStream>(data_.data(), data_.size());
}

std::unique_ptr<CodedInputStream> MemoryFile::GetCodedInputStream(size_t offset, size_t size) {
    if (offset > data_.size()) {
        return std::make_unique<google::protobuf::io::CodedInputStream>(reinterpret_cast<const uint8_t *>(data_.data()), 0);
    }
    return std::make_unique<google::protobuf::io::CodedInputStream>(reinterpret_cast<const uint8_t *>(data_.data()) + offset, std::min(size, data_.size() - offset));
}

std::unique_ptr<ZeroCopyOutputStream> MemoryFile::GetOutputStream() {
  return std::make_unique<google::protobuf::io::StringOutputStream>(&data_);
}

}  // namespace io
}  // namespace fpindex
