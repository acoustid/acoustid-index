// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "in_memory_index.h"

#include <QDebug>
#include <QReadLocker>

namespace Acoustid {

struct InMemoryIndexData {
    InMemoryIndexDocs m_docs;
    InMemoryIndexTerms m_terms;
    QMap<QString, QString> m_attributes;

    void insertDocument(uint32_t docId, const std::vector<uint32_t> &terms);
    void deleteDocument(uint32_t docId);
};

InMemoryIndex::InMemoryIndex() : m_data(std::make_unique<InMemoryIndexData>()) {}

InMemoryIndex::~InMemoryIndex() { QWriteLocker locker(&m_lock); }

void InMemoryIndexData::insertDocument(uint32_t docId, const std::vector<uint32_t> &terms) {
    m_terms.insertDocument(docId, terms);
    m_docs.setActive(docId);
}

void InMemoryIndexData::deleteDocument(uint32_t docId) {
    bool isDeleted;
    if (m_docs.get(docId, isDeleted)) {
        if (isDeleted) {
            return;
        }
        m_terms.deleteDocument(docId);
    }
    m_docs.setDeleted(docId);
}

void InMemoryIndex::clear() {
    QReadLocker locker(&m_lock);
    m_revision = 0;
    m_data->m_docs.clear();
    m_data->m_terms.clear();
    m_data->m_attributes.clear();
}

size_t InMemoryIndex::size() {
    QReadLocker locker(&m_lock);
    return m_data->m_docs.size();
}

std::vector<SearchResult> InMemoryIndex::search(const std::vector<uint32_t> &terms, int64_t timeoutInMSecs) {
    QReadLocker locker(&m_lock);
    return m_data->m_terms.search(terms);
}

QString InMemoryIndex::getAttribute(const QString &name) {
    QReadLocker locker(&m_lock);
    return m_data->m_attributes.value(name);
}

void InMemoryIndex::setAttribute(const QString &name, const QString &value) {
    QWriteLocker locker(&m_lock);
    m_data->m_attributes[name] = value;
}

bool InMemoryIndex::hasAttribute(const QString &name) {
    QReadLocker locker(&m_lock);
    return m_data->m_attributes.contains(name);
}

bool InMemoryIndex::getDocument(uint32_t docId, bool &isDeleted) {
    QReadLocker locker(&m_lock);
    return m_data->m_docs.get(docId, isDeleted);
}

void InMemoryIndex::applyUpdates(const OpBatch &batch) {
    QWriteLocker locker(&m_lock);
    for (auto op : batch) {
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
                m_data->m_attributes[data.name] = data.value;
                break;
            }
        }
    }
}

InMemoryIndexSnapshot InMemoryIndex::snapshot() { return InMemoryIndexSnapshot(&m_lock, m_data.get()); }

const InMemoryIndexDocs &InMemoryIndexSnapshot::docs() const { return m_data->m_docs; }

const InMemoryIndexTerms &InMemoryIndexSnapshot::terms() const { return m_data->m_terms; }

const QMap<QString, QString> &InMemoryIndexSnapshot::attributes() const { return m_data->m_attributes; }

}  // namespace Acoustid
