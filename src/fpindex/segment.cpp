#include "fpindex/segment.h"

#include "fpindex/proto/internal.pb.h"
#include "fpindex/search_result.h"
#include "fpindex/segment_file_format.h"

#include <QDebug>

namespace fpindex {

namespace io {

class File {
 public:
    size_t Size();
    int FileDescriptor();
};

};  // namespace io

class FileInputStream : public google::protobuf::io::ZeroCopyInputStream {
 public:
    FileInputStream(const std::shared_ptr<io::File>& file);

    bool Next(const void** data, int* size) override;
    void BackUp(int count) override;
    bool Skip(int count) override;
    int64_t ByteCount() const override;
};

struct CompareKeyValuePairAndKey {
    bool operator()(const std::pair<uint32_t, uint32_t>& item, uint32_t key) const { return item.first < key; }
    bool operator()(uint32_t key, const std::pair<uint32_t, uint32_t>& item) const { return key < item.first; }
};

bool BlockBasedSegment::Search(const std::vector<uint32_t>& hashes, std::vector<SearchResult>* results) {
    const auto& block_index = GetBlockIndex();
    if (block_index.empty()) {
        return true;
    }
    std::map<uint32_t, uint32_t> scores;

    std::vector<std::pair<uint32_t, uint32_t>> block;
    size_t prev_block_no = SIZE_MAX;

    for (const auto& hash : hashes) {
        auto block_range_start = std::lower_bound(block_index.begin(), block_index.end(), hash);
        if (block_range_start == block_index.end()) {
            if (block_index.front() > hash) {
                // The hash is definitely not in any block.
                continue;
            }
            // We did not find any block that starts with a hash smaller than the current hash.
            // We need to start from the first block.
            block_range_start = block_index.begin();
        } else if (block_range_start != block_index.begin()) {
            // We need to start from the previous block.
            --block_range_start;
        }

        for (auto block_it = block_range_start; block_it != block_index.end(); ++block_it) {
            if (*block_it > hash) {
                // We have passed the block that can contain the hash.
                break;
            }
            const size_t block_no = block_it - block_index.begin();
            qDebug() << "scanning block_no" << block_no;
            if (prev_block_no != block_no) {
                if (!GetBlock(block_no, &block)) {
                    return false;
                }
                prev_block_no = block_no;
            }

            auto items = std::equal_range(block.begin(), block.end(), hash, CompareKeyValuePairAndKey{});
            for (auto i = items.first; i != items.second; ++i) {
                scores[i->second]++;
            }
        }
    }

    results->clear();
    results->reserve(scores.size());
    for (auto score : scores) {
        results->emplace_back(score.first, score.second);
    }
    return true;
}

bool Segment::Search(const std::vector<uint32_t>& hashes, std::vector<SearchResult>* results) {
    if (!ready_) {
        return false;
    }
    return BlockBasedSegment::Search(hashes, results);
}

const std::vector<uint32_t>& Segment::GetBlockIndex() { return block_index_; }

bool Segment::GetBlock(size_t block_no, std::vector<std::pair<uint32_t, uint32_t>>* items) { return false; }

/*

bool Segment::GetBlock(size_t block_no, std::vector<std::pair<uint32_t, uint32_t>>* items) {
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

*/

}  // namespace fpindex
