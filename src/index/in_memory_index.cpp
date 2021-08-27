// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <QReadLocker>

#include "in_memory_index.h"

namespace Acoustid {

InMemoryIndex::InMemoryIndex() : m_data(QSharedPointer<InMemoryIndexData>()) {
}

void InMemoryIndex::search(const uint32_t *fingerprint, size_t length, Collector *collector, int64_t timeoutInMSecs) {
    QReadLocker locker(&m_data->lock);
    const auto index = m_data->index;
    for (size_t i = 0; i < length; i++) {
        const auto key = fingerprint[i];
        QHash<uint32_t, uint32_t>::const_iterator valuesIter = index.find(key);
        while (valuesIter != index.end() && valuesIter.key() == key) {
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

} // namespace Acoustid
