// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <QSqlQuery>

#include "multi_layer_index.h"

namespace Acoustid {

MultiLayerIndex::MultiLayerIndex() : m_inMemoryIndex(QSharedPointer<InMemoryIndex>::create()) {
}

MultiLayerIndex::~MultiLayerIndex() {
}

bool MultiLayerIndex::isOpen() {
    return m_db.isValid() && m_db.isOpen() && m_persistentIndex;
}

int MultiLayerIndex::getDatabaseSchemaVersion() {
    QSqlQuery query(m_db);
    query.exec("SELECT version FROM schema_version");
    if (query.first()) {
        return query.value(0).toInt();
    } else {
        return 0;
    }
}

void MultiLayerIndex::updateDatabaseSchemaVersion(int version) {
    QSqlQuery query(m_db);
    query.prepare("UPDATE schema_version SET version = ?");
    query.bindValue(0, version);
    query.exec();
    if (query.numRowsAffected() == 0) {
        query.prepare("INSERT INTO schema_version (id, version) VALUES (1, ?)");
        query.bindValue(0, version);
        query.exec();
    }
}

void MultiLayerIndex::upgradeDatabaseSchemaV1() {
    QSqlQuery query(m_db);
    query.exec("CREATE TABLE schema_version (id INTEGER PRIMARY KEY, version INTEGER)");
    query.exec("CREATE TABLE oplog (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, op TEXT, data TEXT)");
}

void MultiLayerIndex::upgradeDatabaseSchema() {
    const auto latestSchemaVersion = 1;
    auto schemaVersion = getDatabaseSchemaVersion();
    while (schemaVersion < latestSchemaVersion) {
        schemaVersion++;
        qDebug() << "Upgrading to schema version" << schemaVersion;
        switch (schemaVersion) {
            case 1:
                upgradeDatabaseSchemaV1();
                break;
        }
        updateDatabaseSchemaVersion(schemaVersion);
    }
}

void MultiLayerIndex::open(QSharedPointer<Directory> dir, bool create) {
    m_db = dir->openDatabase("control.db");
    upgradeDatabaseSchema();

    m_persistentIndex = QSharedPointer<Index>::create(dir, create);
}

bool MultiLayerIndex::containsDocument(uint32_t docId) {
    assert(isOpen());
    return m_inMemoryIndex->containsDocument(docId);
}

bool MultiLayerIndex::deleteDocument(uint32_t docId) {
    assert(isOpen());
    return m_inMemoryIndex->deleteDocument(docId);
}

bool MultiLayerIndex::insertOrUpdateDocument(uint32_t docId, const QVector<uint32_t> &terms) {
    assert(isOpen());
    return m_inMemoryIndex->insertOrUpdateDocument(docId, terms);
}

void MultiLayerIndex::search(const QVector<uint32_t> &terms, Collector *collector, int64_t timeoutInMSecs) {
    assert(isOpen());
    m_inMemoryIndex->search(terms, collector, timeoutInMSecs);
    m_persistentIndex->search(terms, collector, timeoutInMSecs);
}

bool MultiLayerIndex::hasAttribute(const QString &name) {
    assert(isOpen());
    return m_inMemoryIndex->hasAttribute(name);
}

QString MultiLayerIndex::getAttribute(const QString &name) {
    assert(isOpen());
    return m_inMemoryIndex->getAttribute(name);
}

void MultiLayerIndex::setAttribute(const QString &name, const QString &value) {
    assert(isOpen());
    m_inMemoryIndex->setAttribute(name, value);
}

void MultiLayerIndex::applyUpdates(OpStream *updates) {
    assert(isOpen());
    m_inMemoryIndex->applyUpdates(updates);
}

} // namespace Acoustid

