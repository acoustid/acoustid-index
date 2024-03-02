#include "fpindex/segment.h"

#include "fpindex/proto/internal.pb.h"
#include "fpindex/search_result.h"
#include "fpindex/segment_file_format.h"

namespace fpindex {

namespace io {

class File {
 public:
    size_t Size();
    int FileDescriptor();
};

};

class FileInputStream : public google::protobuf::io::ZeroCopyInputStream {
 public:
    FileInputStream(const std::shared_ptr<io::File>& file);

    bool Next(const void** data, int* size) override;
    void BackUp(int count) override;
    bool Skip(int count) override;
    int64_t ByteCount() const override;
};

bool Segment::Search(const std::vector<uint32_t>& hashes, std::vector<SearchResult>* results) {
    if (!ready_) {
        return false;
    }
    results->clear();
    return true;
}

bool Segment::ReadBlock(size_t block_no, std::vector<std::pair<uint32_t, uint32_t>>* items) {
    if (block_no >= block_index_.size()) {
        return false;
    }
    const size_t block_offset = first_block_offset_ + block_no * header_.block_size();
    FileInputStream stream(file_);
    if (!stream.Skip(block_offset)) {
        return false;
    }
    google::protobuf::io::CodedInputStream coded_stream(&stream);
    return internal::ParseSegmentBlock(&coded_stream, header_, items);
}

bool Segment::Load(const std::shared_ptr<io::File>& file) {
    if (ready_) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);

    auto stream = std::make_unique<google::protobuf::io::FileInputStream>(file->FileDescriptor());
    auto coded_stream = std::make_unique<google::protobuf::io::CodedInputStream>(stream.get());

    if (!internal::ParseSegmentHeader(coded_stream.get(), &header_)) {
        return false;
    }

    const size_t file_size = file->Size();
    const size_t header_size = coded_stream->CurrentPosition();

    if (header_size > file_size) {
        return false;
    }

    const size_t block_size = header_.block_size();
    const size_t block_count = (file_size - header_size) / block_size;

    if (block_count * block_size + header_size != file->Size()) {
        return false;
    }

    first_block_offset_ = header_size;

    block_index_.clear();
    block_index_.reserve(block_count);

    std::vector<std::pair<uint32_t, uint32_t>> items;
    for (size_t i = 0; i < block_count; ++i) {
        if (!internal::ParseSegmentBlock(coded_stream.get(), header_, &items)) {
            return false;
        }
        if (items.empty()) {
            return false;
        }
        block_index_.push_back(items.front().first);
    }

    if (block_index_.size() != block_count) {
        return false;
    }

    file_ = file;
    ready_ = true;
    return true;
}

}  // namespace fpindex
