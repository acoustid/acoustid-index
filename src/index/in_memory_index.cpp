// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "in_memory_index.h"

#include <QDebug>
#include <QReadLocker>

namespace Acoustid {

struct InMemoryIndexData {
    InMemoryIndexDocs docs;
    QMultiHash<uint32_t, uint32_t> index;
    QMap<QString, QString> attributes;
    OpBatch updates;

    void insertDocument(uint32_t docId, const std::vector<uint32_t> &terms);
    void deleteDocument(uint32_t docId);
};

InMemoryIndex::InMemoryIndex() : m_data(std::make_unique<InMemoryIndexData>()) {}

InMemoryIndex::~InMemoryIndex() { QWriteLocker locker(&m_lock); }

void InMemoryIndexData::insertDocument(uint32_t docId, const std::vector<uint32_t> &terms) {
    for (size_t i = 0; i < terms.size(); i++) {
        const auto term = terms[i];
        index.insert(term, docId);
    }
    docs.setActive(docId);
}

void InMemoryIndexData::deleteDocument(uint32_t docId) {
    bool isDeleted;
    if (docs.get(docId, isDeleted)) {
        if (isDeleted) {
            return;
        }
        for (auto it = index.begin(); it != index.end();) {
            if (it.value() == docId) {
                it = index.erase(it);
            } else {
                ++it;
            }
        }
    }
    docs.setDeleted(docId);
}

void InMemoryIndex::clear() {
    QReadLocker locker(&m_lock);
    m_revision = 0;
    m_data->docs.clear();
    m_data->index.clear();
    m_data->attributes.clear();
    m_data->updates.clear();
}

size_t InMemoryIndex::size() {
    QReadLocker locker(&m_lock);
    return m_data->docs.size();
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
        results.emplace_back(it.key(), it.value(), m_revision);
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
    return m_data->docs.get(docId, isDeleted);
}

void InMemoryIndex::applyUpdates(const OpBatch &batch) {
    QWriteLocker locker(&m_lock);
    for (auto op : batch) {
        m_data->updates.add(op);
        switch (op.type()) {
            case INSERT_OR_UPDATE_DOCUMENT: {
                auto data = op.data<InsertOrUpdateDocument>();
                m_data->deleteDocument(data.docId);
                m_data->insertDocument(data.docId, data.terms);
                break;
            }
            case DELETE_DOCUMENT: {
                auto data = op.data<DeleteDocument>();
                m_data->deleteDocument(data.docId);
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

const OpBatch &InMemoryIndex::updates() {
    QReadLocker locker(&m_lock);
    return m_data->updates;
}

InMemoryIndexSnapshot InMemoryIndex::snapshot() { return InMemoryIndexSnapshot(&m_lock, m_data.get()); }

const InMemoryIndexDocs &InMemoryIndexSnapshot::docs() const { return m_data->docs; }

const QMap<QString, QString> &InMemoryIndexSnapshot::attributes() const { return m_data->attributes; }

}  // namespace Acoustid
