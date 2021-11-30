// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <QReadLocker>

#include "in_memory_index.h"

namespace Acoustid {

InMemoryIndex::InMemoryIndex() : m_data(QSharedPointer<InMemoryIndexData>::create()) {
}

InMemoryIndex::~InMemoryIndex() {
}

void InMemoryIndexData::insertInternal(uint32_t docId, const QVector<uint32_t> &terms) {
    for (size_t i = 0; i < terms.size(); i++) {
        const auto term = terms[i];
        index.insert(term, docId);
    }
    docs.insert(docId, true);
}

bool InMemoryIndexData::deleteInternal(uint32_t docId) {
    auto it = docs.find(docId);
    if (it == docs.end()) {
        docs.insert(docId, false);
        return false;
    }
    auto isActive = *it;
    if (!isActive) {
        *it = false;
        return false;
    }
    *it = false;
    auto i = index.begin();
    while (i != index.end()) {
        if (i.value() == docId) {
            i = index.erase(i);
        } else {
            ++i;
        }
    }
    return true;
}

void InMemoryIndex::reset() {
    QReadLocker locker(&m_data->lock);
    m_data->docs.clear();
    m_data->index.clear();
    m_data->attributes.clear();
}

bool InMemoryIndex::isDocumentDeleted(uint32_t docId) {
    QReadLocker locker(&m_data->lock);
    auto it = m_data->docs.find(docId);
    if (it == m_data->docs.end()) {
        return false;
    }
    auto isActive = *it;
    return !isActive;
}

bool InMemoryIndex::containsDocument(uint32_t docId) {
    QReadLocker locker(&m_data->lock);
    return m_data->docs.value(docId);
}

bool InMemoryIndex::deleteDocument(uint32_t docId) {
    QWriteLocker locker(&m_data->lock);
    return m_data->deleteInternal(docId);
}

bool InMemoryIndex::insertOrUpdateDocument(uint32_t docId, const QVector<uint32_t> &terms) {
    QWriteLocker locker(&m_data->lock);
    auto updated = m_data->deleteInternal(docId);
    m_data->insertInternal(docId, terms);
    return updated;
}

QVector<SearchResult> InMemoryIndex::search(const QVector<uint32_t> &terms, int64_t timeoutInMSecs) {
    QReadLocker locker(&m_data->lock);
    const auto &index = m_data->index;
    QHash<uint32_t, int> hits;
    for (size_t i = 0; i < terms.size(); i++) {
        const auto term = terms[i];
        QHash<uint32_t, uint32_t>::const_iterator valuesIter = index.find(term);
        while (valuesIter != index.end() && valuesIter.key() == term) {
            auto docId = valuesIter.value();
            hits[docId]++;
            ++valuesIter;
        }
    }
    QVector<SearchResult> results;
    for (auto it = hits.begin(); it != hits.end(); ++it) {
        results.append(SearchResult(it.key(), it.value()));
    }
    std::sort(results.begin(), results.end(), [](const SearchResult& a, const SearchResult& b) {
        return a.score() >= b.score();
    });
    return results;
}

bool InMemoryIndex::hasAttribute(const QString &name) {
    QReadLocker locker(&m_data->lock);
    return m_data->attributes.contains(name);
}

QString InMemoryIndex::getAttribute(const QString &name) {
    QReadLocker locker(&m_data->lock);
    return m_data->attributes.value(name);
}

void InMemoryIndex::setAttribute(const QString &name, const QString &value) {
    QWriteLocker locker(&m_data->lock);
    m_data->attributes[name] = value;
}

void InMemoryIndex::applyUpdates(const OpBatch &batch) {
    QWriteLocker locker(&m_data->lock);
    for (auto op : batch) {
        switch (op.type()) {
            case INSERT_OR_UPDATE_DOCUMENT:
                {
                    auto data = std::get<InsertOrUpdateDocument>(op.data());
                    m_data->deleteInternal(data.docId);
                    m_data->insertInternal(data.docId, data.terms);
                }
                break;
            case DELETE_DOCUMENT:
                {
                    auto data = std::get<DeleteDocument>(op.data());
                    m_data->deleteInternal(data.docId);
                }
                break;
            case SET_ATTRIBUTE:
                {
                    auto data = std::get<SetAttribute>(op.data());
                    m_data->attributes[data.name] = data.value;
                }
                break;
        }
    }
}

} // namespace Acoustid
