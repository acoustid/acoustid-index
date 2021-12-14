// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "in_memory_index.h"

#include <QDebug>
#include <QReadLocker>

namespace Acoustid {

struct InMemoryIndexData {
    QHash<uint32_t, bool> docs;
    QMultiHash<uint32_t, uint32_t> index;
    QHash<QString, QString> attributes;

    void insertInternal(uint32_t docId, const std::vector<uint32_t> &terms);
    bool deleteInternal(uint32_t docId);
};

InMemoryIndex::InMemoryIndex() : m_data(std::make_unique<InMemoryIndexData>()) {}

InMemoryIndex::~InMemoryIndex() {}

void InMemoryIndexData::insertInternal(uint32_t docId, const std::vector<uint32_t> &terms) {
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

void InMemoryIndex::clear() {
    QReadLocker locker(&m_lock);
    m_data->docs.clear();
    m_data->index.clear();
    m_data->attributes.clear();
}

std::vector<SearchResult> InMemoryIndex::search(const std::vector<uint32_t> &terms, int64_t timeoutInMSecs) {
    QReadLocker locker(&m_lock);
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
    std::vector<SearchResult> results;
    for (auto it = hits.begin(); it != hits.end(); ++it) {
        results.emplace_back(it.key(), it.value());
    }
    std::sort(results.begin(), results.end(), [](const SearchResult &a, const SearchResult &b) {
        return a.score() >= b.score();
    });
    return results;
}

QString InMemoryIndex::getAttribute(const QString &name) {
    QReadLocker locker(&m_lock);
    return m_data->attributes.value(name);
}

void InMemoryIndex::setAttribute(const QString &name, const QString &value) {
    QWriteLocker locker(&m_lock);
    m_data->attributes[name] = value;
}

bool InMemoryIndex::hasAttribute(const QString &name) {
    QReadLocker locker(&m_lock);
    return m_data->attributes.contains(name);
}

bool InMemoryIndex::getDocument(uint32_t docId, bool &isDeleted) {
    QReadLocker locker(&m_lock);
    auto it = m_data->docs.find(docId);
    if (it == m_data->docs.end()) {
        return false;
    }
    isDeleted = !*it;
    return true;
}

void InMemoryIndex::applyUpdates(const OpBatch &batch) {
    QWriteLocker locker(&m_lock);
    for (auto op : batch) {
        switch (op.type()) {
            case INSERT_OR_UPDATE_DOCUMENT: {
                auto data = op.data<InsertOrUpdateDocument>();
                m_data->deleteInternal(data.docId);
                m_data->insertInternal(data.docId, data.terms);
                break;
            }
            case DELETE_DOCUMENT: {
                auto data = op.data<DeleteDocument>();
                m_data->deleteInternal(data.docId);
                break;
            }
            case SET_ATTRIBUTE: {
                auto data = op.data<SetAttribute>();
                m_data->attributes[data.name] = data.value;
                break;
            }
        }
    }
}

}  // namespace Acoustid
