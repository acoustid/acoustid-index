// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <QMutexLocker>

#include "in_memory_index.h"

namespace Acoustid {

InMemoryIndex::InMemoryIndex() : m_data(QSharedPointer<InMemoryIndexData>()) {
}

InMemoryIndex::~InMemoryIndex() {
}

QString InMemoryIndex::getAttribute(const QString &name) {
    QMutexLocker locker(&m_data->mutex);
    return m_data->attributes.value(name);
}

} // namespace Acoustid
