// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "multi_index.h"

namespace Acoustid {

MultiIndex::MultiIndex(const QSharedPointer<Directory> &dir) : m_dir(dir) {}

void MultiIndex::close() {
    QMutexLocker locker(&m_mutex);
    for (auto &index : m_indexes) {
        index->close();
    }
    m_indexes.clear();
}

bool MultiIndex::indexExists(const QString &name) {
    QMutexLocker locker(&m_mutex);
    if (m_indexes.contains(name)) {
        return true;
    }
    auto subDir = QSharedPointer<Directory>(m_dir->openDirectory(name));
    return Index::exists(subDir);
}

QSharedPointer<Index> MultiIndex::getIndex(const QString &name, bool create) {
    QMutexLocker locker(&m_mutex);
    auto index = m_indexes.value(name);
    if (index) {
        return index;
    }
    auto subDir = QSharedPointer<Directory>(m_dir->openDirectory(name));
    index = QSharedPointer<Index>::create(subDir, create);
    m_indexes.insert(name, index);
    return index;
}

void MultiIndex::createIndex(const QString &name) {
    getIndex(name, true);
}

void MultiIndex::deleteIndex(const QString &name) {
    QMutexLocker locker(&m_mutex);
    m_indexes.remove(name);
    m_dir->deleteDirectory(name);
}

}  // namespace Acoustid
