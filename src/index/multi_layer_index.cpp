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

void MultiLayerIndex::updateDatabaseSchema() {
    QSqlQuery query(m_db);
    auto tables = m_db.tables(QSql::Tables);

    int currentSchemaVersion;
    if (tables.contains("schema_version")) {
        query.exec("SELECT version FROM schema_version");
        if (query.first()) {
            currentSchemaVersion = query.value(0).toInt();
        } else {
            currentSchemaVersion = 0;
        }
    } else {
        query.exec("CREATE TABLE schema_version (version INT PRIMARY KEY)");
        currentSchemaVersion = 0;
    }

    auto targetSchemaVersion = 1;

    for (int schemaVersion = currentSchemaVersion + 1; schemaVersion <= targetSchemaVersion; schemaVersion++) {
        qDebug() << "Upgradin to schema version" << schemaVersion;
        if (currentSchemaVersion != targetSchemaVersion) {
            query.prepare("UPDATE schema_version SET version = ?");
            query.bindValue(0, targetSchemaVersion);
            query.exec();
            if (query.numRowsAffected() == 0) {
                query.prepare("INSERT INTO schema_version (version) VALUES (?)");
                query.bindValue(0, targetSchemaVersion);
                query.exec();
            }
        }
    }
}

void MultiLayerIndex::open(QSharedPointer<Directory> dir, bool create) {
    m_db = dir->openDatabase("control.db");
    updateDatabaseSchema();

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

} // namespace Acoustid

