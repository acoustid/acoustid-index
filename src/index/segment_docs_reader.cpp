// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "segment_docs_reader.h"

#include "segment_docs.h"
#include "store/input_stream.h"

namespace Acoustid {

SegmentDocsReader::SegmentDocsReader(InputStream *input) : m_input(input) {}

std::shared_ptr<SegmentDocs> SegmentDocsReader::read() {
    auto docs = std::make_shared<SegmentDocs>();
    while (true) {
        auto docId = m_input->readInt32();
        if (docId == 0) {
            break;
        }
        auto version = m_input->readInt32();
        auto isDeleted = m_input->readByte() == 1;
        docs->add(docId, version, isDeleted);
    }
    return docs;
}

}  // namespace Acoustid
