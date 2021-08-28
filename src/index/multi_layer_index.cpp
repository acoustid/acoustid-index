// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <QReadLocker>

#include "multi_layer_index.h"

namespace Acoustid {

MultiLayerIndex::MultiLayerIndex() : m_inMemoryIndex(QSharedPointer<InMemoryIndex>::create()) {
}

MultiLayerIndex::~MultiLayerIndex() {
}

bool MultiLayerIndex::containsDocument(uint32_t docId) {
    return m_inMemoryIndex->containsDocument(docId);
}

bool MultiLayerIndex::deleteDocument(uint32_t docId) {
    return m_inMemoryIndex->deleteDocument(docId);
}

bool MultiLayerIndex::insertOrUpdateDocument(uint32_t docId, const QVector<uint32_t> &terms) {
    return m_inMemoryIndex->insertOrUpdateDocument(docId, terms);
}

void MultiLayerIndex::search(const QVector<uint32_t> &terms, Collector *collector, int64_t timeoutInMSecs) {
    m_inMemoryIndex->search(terms, collector, timeoutInMSecs);
    m_persistentIndex->search(terms, collector, timeoutInMSecs);
}

bool MultiLayerIndex::hasAttribute(const QString &name) {
    return m_inMemoryIndex->hasAttribute(name);
}

QString MultiLayerIndex::getAttribute(const QString &name) {
    return m_inMemoryIndex->getAttribute(name);
}

void MultiLayerIndex::setAttribute(const QString &name, const QString &value) {
    m_inMemoryIndex->setAttribute(name, value);
}

} // namespace Acoustid

