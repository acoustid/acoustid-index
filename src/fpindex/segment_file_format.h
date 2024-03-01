#pragma once

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/util/delimited_message_util.h>

#include "fpindex/proto/internal.pb.h"

namespace fpindex {
namespace internal {

namespace io = google::protobuf::io;
using google::protobuf::util::ParseDelimitedFromCodedStream;
using google::protobuf::util::SerializeDelimitedToCodedStream;

static constexpr uint32_t SEGMENT_HEADER_MAGIC = 0x22de521c;
static constexpr int DEFAULT_BLOCK_SIZE = 4096;

inline void SerializeSegmentHeader(io::CodedOutputStream *output, const SegmentHeader &header) {
    output->WriteLittleEndian32(SEGMENT_HEADER_MAGIC);
    SerializeDelimitedToCodedStream(header, output);
}

inline bool ParseSegmentHeader(io::CodedInputStream *input, SegmentHeader *header) {
    uint32_t magic;
    if (!input->ReadLittleEndian32(&magic)) {
        return false;
    }
    if (magic != SEGMENT_HEADER_MAGIC) {
        return false;
    }

    if (!ParseDelimitedFromCodedStream(header, input, NULL)) {
        return false;
    }
    return true;
}

template <typename Iter>
inline Iter SerializeSegmentBlock(io::CodedOutputStream *output, int block_size, Iter begin, Iter end) {
    const int last_block_offset = output->ByteCount();

    const int final_entry_size = (io::CodedOutputStream::VarintSize32(0) + io::CodedOutputStream::VarintSize32(0));

    uint32_t last_key = 0;
    uint32_t last_value = 0;
    for (auto it = begin; it != end; ++it) {
        uint32_t key = it->first;
        uint32_t value = it->second;

        uint32_t key_diff = key - last_key;
        uint32_t value_diff = value - last_value;

        const int padding_size = block_size - final_entry_size - (output->ByteCount() - last_block_offset);
        const int entry_size =
            (io::CodedOutputStream::VarintSize32(key_diff) + io::CodedOutputStream::VarintSize32(value_diff));
        if (padding_size < entry_size) {
            output->WriteVarint32(0);
            output->WriteVarint32(0);
            for (int i = 0; i < padding_size; i++) {
                output->WriteRaw("\0", 1);
            }
            return it;
        }

        output->WriteVarint32(key_diff);
        output->WriteVarint32(value_diff);

        if (output->HadError()) {
            return it;
        }

        last_key = key;
        last_value = value;
    }

    const int padding_size = block_size - final_entry_size - (output->ByteCount() - last_block_offset);
    output->WriteVarint32(0);
    output->WriteVarint32(0);
    for (int i = 0; i < padding_size; i++) {
        output->WriteRaw("\0", 1);
    }
    return end;
}

bool ParseSegmentBlock(io::CodedInputStream *input, int block_size,
                       std::vector<std::pair<uint32_t, uint32_t>> *entries) {
    auto start = input->CurrentPosition();
    auto limit = input->PushLimit(block_size);
    uint32_t key = 0;
    uint32_t value = 0;
    entries->clear();
    while (input->BytesUntilLimit() > 0) {
        uint32_t key_diff;
        uint32_t value_diff;
        if (!input->ReadVarint32(&key_diff)) {
            return false;
        }
        if (!input->ReadVarint32(&value_diff)) {
            return false;
        }
        if (key_diff == 0 && value_diff == 0) {
            break;
        }
        key += key_diff;
        value += value_diff;
        entries->emplace_back(key, value);
    }
    const int padding_size = block_size - (input->CurrentPosition() - start);
    if (padding_size > 0) {
        input->Skip(padding_size);
    }
    input->PopLimit(limit);
    return true;
}

}  // namespace internal
}  // namespace fpindex
