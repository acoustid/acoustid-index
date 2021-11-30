// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_DOCS_H_
#define ACOUSTID_INDEX_SEGMENT_DOCS_H_

#include <QSharedPointer>
#include "common.h"

namespace Acoustid {

class VersionedDocId
{
 public:
    VersionedDocId(uint32_t docId, uint32_t version, bool isDeleted)
        : m_docId(docId), m_version(version), m_isDeleted(isDeleted) {}

    uint32_t docId() const { return m_docId; }
    uint32_t version() const { return m_version; }
    bool isDeleted() const { return m_isDeleted; }

    bool isValid() const { return m_docId != 0; }

    bool operator==(const VersionedDocId &other) const
    {
        return m_docId == other.m_docId && m_version == other.m_version && m_isDeleted == other.m_isDeleted;
    }

    bool operator!=(const VersionedDocId &other) const
    {
        return !(*this == other);
    }

    bool operator<(const VersionedDocId &other) const
    {
        if (m_docId < other.m_docId) {
            return true;
        } else if (m_docId > other.m_docId) {
            return false;
        } else if (m_version < other.m_version) {
            return true;
        } else if (m_version > other.m_version) {
            return false;
        } else {
            return m_isDeleted < other.m_isDeleted;
        }
    }

 private:
    uint32_t m_docId;
    uint32_t m_version;
    bool m_isDeleted;
};

class SegmentDocsIterator
{
 public:
    SegmentDocsIterator(std::map<uint32_t, std::pair<uint32_t, bool>>::const_iterator it)
        : m_it(it) {}

    VersionedDocId operator*() const { return VersionedDocId(m_it->first, m_it->second.first, m_it->second.second); }

    bool operator==(const SegmentDocsIterator &other) const { return m_it == other.m_it; }
    bool operator!=(const SegmentDocsIterator &other) const { return m_it != other.m_it; }

    SegmentDocsIterator &operator++() { ++m_it; return *this; }
    SegmentDocsIterator operator++(int) { SegmentDocsIterator it = *this; ++m_it; return it; }

 private:
    std::map<uint32_t, std::pair<uint32_t, bool>>::const_iterator m_it;
};

class SegmentDocs
{
public:
	SegmentDocs();
	virtual ~SegmentDocs();

    bool containsDocument(uint32_t docId) const {
        auto it = m_docs.find(docId);
        if (it == m_docs.end()) {
            return false;
        }
        return !it->second.second;
    }

    uint32_t getVersion(uint32_t docId) const {
        auto it = m_docs.find(docId);
        if (it == m_docs.end()) {
            return 0;
        }
        return it->second.first;
    }

    VersionedDocId get(uint32_t docId) const {
        auto it = m_docs.find(docId);
        if (it == m_docs.end()) {
            return VersionedDocId(0, 0, true);
        }
        return VersionedDocId(it->first, it->second.first, it->second.second);
    }

    std::optional<std::pair<uint32_t, bool>> findDocumentUpdate(uint32_t docId, uint32_t currentVersion) const;

    void reserve(size_t count) {
    }

    void add(uint32_t docId, uint32_t version, bool isDeleted) {
        auto it = m_docs.find(docId);
        if (it == m_docs.end()) {
            qDebug() << "Adding new document" << docId << version << isDeleted;
            m_docs.insert({docId, {version, isDeleted}});
        } else {
            if (it->second.first < version) {
                qDebug() << "Updating document" << docId << version << isDeleted;
                it->second.first = version;
                it->second.second = isDeleted;
            } else {
                qDebug() << "Ignoring document" << docId << version << isDeleted;
            }
        }
    }

    typedef SegmentDocsIterator const_iterator;

    const_iterator begin() const {
        return SegmentDocsIterator(m_docs.begin());
    }

    const_iterator end() const {
        return SegmentDocsIterator(m_docs.end());
    }

private:
    std::map<uint32_t, std::pair<uint32_t, bool>> m_docs;
};

}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_SEGMENT_DOCS_H_
