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

inline void InitializeSegmentHeader(SegmentHeader *header) {
    header->set_block_format(SegmentBlockFormat::BLOCK_FORMAT_V1);
    header->set_block_size(DEFAULT_BLOCK_SIZE);
}

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
inline Iter SerializeSegmentBlock(io::CodedOutputStream *output, const SegmentHeader &header, Iter begin, Iter end) {
    const int last_block_offset = output->ByteCount();
    const int block_size = header.block_size();

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

inline bool ParseSegmentBlockV1(io::CodedInputStream *input, const SegmentHeader &header,
                         std::vector<std::pair<uint32_t, uint32_t>> *entries) {
    const int block_size = header.block_size();
    auto start = input->CurrentPosition();
    auto limit = input->PushLimit(block_size);
    uint32_t key = 0;
    uint32_t value = 0;
    bool found_end_marker = false;
    entries->clear();
    entries->reserve(block_size / 4);
    while (true) {
        uint32_t key_diff;
        uint32_t value_diff;
        if (!input->ReadVarint32(&key_diff)) {
            input->PopLimit(limit);
            return false;
        }
        if (!input->ReadVarint32(&value_diff)) {
            input->PopLimit(limit);
            return false;
        }
        if (key_diff == 0 && value_diff == 0) {
            found_end_marker = true;
            break;
        }
        key += key_diff;
        value += value_diff;
        entries->emplace_back(key, value);
    }
    if (!found_end_marker) {
        return false;
    }
    const int padding_size = block_size - (input->CurrentPosition() - start);
    if (padding_size > 0) {
        input->Skip(padding_size);
    }
    input->PopLimit(limit);
    return true;
}

inline bool ParseSegmentBlock(io::CodedInputStream *input, const SegmentHeader &header,
                       std::vector<std::pair<uint32_t, uint32_t>> *entries) {
    if (header.block_format() == SegmentBlockFormat::BLOCK_FORMAT_V1) {
        return ParseSegmentBlockV1(input, header, entries);
    }
    return false;
}

}  // namespace internal
}  // namespace fpindex
