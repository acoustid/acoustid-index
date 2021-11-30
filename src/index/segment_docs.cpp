// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "segment_docs.h"

namespace Acoustid {

SegmentDocs::SegmentDocs() {}

SegmentDocs::~SegmentDocs() {}

std::optional<std::pair<uint32_t, bool>> SegmentDocs::findDocumentUpdate(uint32_t docId, uint32_t currentVersion) const {
    auto it = m_docs.find(docId);
    qDebug() << "it" << *it;
    if (it == m_docs.end()) {
        qDebug() << docId << "not found";
        return std::nullopt;
    }
    if (currentVersion >= it->second.first) {
        qDebug() << docId << "too old";
        return std::nullopt;
    }
    return std::make_pair(it->second.first, it->second.second);
}

}  // namespace Acoustid
