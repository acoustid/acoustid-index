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
    docs.insert(docId);
}

bool InMemoryIndexData::deleteInternal(uint32_t docId) {
    auto removed = docs.remove(docId);
    if (removed) {
        auto i = index.begin();
        while (i != index.end()) {
            if (i.value() == docId) {
                i = index.erase(i);
            } else {
                ++i;
            }
        }
    }
    return removed;
}

bool InMemoryIndex::containsDocument(uint32_t docId) {
    QReadLocker locker(&m_data->lock);
    return m_data->docs.contains(docId);
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

void InMemoryIndex::search(const QVector<uint32_t> &terms, Collector *collector, int64_t timeoutInMSecs) {
    QReadLocker locker(&m_data->lock);
    const auto &index = m_data->index;
    for (size_t i = 0; i < terms.size(); i++) {
        const auto term = terms[i];
        QHash<uint32_t, uint32_t>::const_iterator valuesIter = index.find(term);
        while (valuesIter != index.end() && valuesIter.key() == term) {
            collector->collect(valuesIter.value());
            ++valuesIter;
        }
    }
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

} // namespace Acoustid
