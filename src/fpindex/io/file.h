#pragma once

#include <memory>

namespace google {
namespace protobuf {
namespace io {
class ZeroCopyInputStream;
class ZeroCopyOutputStream;
class CodedInputStream;
class CodedOutputStream;
}  // namespace io
}  // namespace protobuf
}  // namespace google

namespace fpindex {
namespace io {

using google::protobuf::io::ZeroCopyInputStream;
using google::protobuf::io::ZeroCopyOutputStream;
using google::protobuf::io::CodedInputStream;
using google::protobuf::io::CodedOutputStream;

class File {
 public:
    virtual size_t Size() = 0;

    virtual std::unique_ptr<ZeroCopyInputStream> GetInputStream() = 0;
    virtual std::unique_ptr<ZeroCopyOutputStream> GetOutputStream() = 0;

    virtual std::unique_ptr<CodedInputStream> GetCodedInputStream(size_t offset, size_t size) = 0;
};

}  // namespace io
}  // namespace fpindex
