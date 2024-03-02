#pragma once

#include <memory>
#include <string>

#include "fpindex/io/file.h"

namespace fpindex {
namespace io {

class Directory {
 public:
  virtual std::shared_ptr<File> OpenFile(const std::string& name, bool create = false) = 0;
};

}  // namespace io
}  // namespace fpindex
