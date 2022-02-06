// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "multi_index.h"

namespace Acoustid {

MultiIndex::MultiIndex() {}

void MultiIndex::close() {
    QMutexLocker locker(&m_mutex);
    for (auto &index : m_indexes) {
        index->close();
    }
    m_indexes.clear();
}

void MultiIndex::addIndex(const QString &name, const QSharedPointer<Index> &index) {
    QMutexLocker locker(&m_mutex);
    m_indexes.insert(name, index);
}

QThreadPool *MultiIndex::threadPool() const { return m_threadPool; }

void MultiIndex::setThreadPool(QThreadPool *threadPool) {
    for (auto &index : m_indexes) {
        index->setThreadPool(threadPool);
    }
    m_threadPool = threadPool;
}

bool MultiIndex::indexExists(const QString &name) {
    QMutexLocker locker(&m_mutex);
    if (m_indexes.contains(name)) {
        return true;
    }
    return false;
}

QSharedPointer<Index> MultiIndex::getIndex(const QString &name, bool create) {
    QMutexLocker locker(&m_mutex);
    auto index = m_indexes.value(name);
    if (index) {
        return index;
    }
    if (!create) {
        throw IndexNotFoundException("Index does not exist");
    }
    throw Exception("Index creation is not supported");
}

void MultiIndex::createIndex(const QString &name) {
    QMutexLocker locker(&m_mutex);
    if (m_indexes.contains(name)) {
        return;
    }
    throw Exception("Index creation is not supported");
}

void MultiIndex::deleteIndex(const QString &name) {
    throw Exception("Index deletion is not supported");
}

}  // namespace Acoustid
