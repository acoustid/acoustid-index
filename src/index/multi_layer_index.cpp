// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "multi_layer_index.h"

#include <QDateTime>
#include <QSqlQuery>

#include "oplog.pb.h"

namespace Acoustid {

MultiLayerIndex::MultiLayerIndex() : m_inMemoryIndex(QSharedPointer<InMemoryIndex>::create()) {}

MultiLayerIndex::~MultiLayerIndex() {}

bool MultiLayerIndex::isOpen() { return m_db.isValid() && m_db.isOpen() && m_persistentIndex; }

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
    query.exec(
        "CREATE TABLE schema_version (id INTEGER PRIMARY KEY, version "
        "INTEGER)");
    query.exec(
        "CREATE TABLE oplog (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, "
        "op TEXT, data TEXT)");
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

bool MultiLayerIndex::containsDocument(uint32_t docId) { return m_inMemoryIndex->containsDocument(docId) || m_persistentIndex->containsDocument(docId); }

QVector<SearchResult> MultiLayerIndex::search(const QVector<uint32_t> &terms, int64_t timeoutInMSecs) {
    assert(isOpen());
    auto results = m_inMemoryIndex->search(terms, timeoutInMSecs);
    results.append(m_persistentIndex->search(terms, timeoutInMSecs));
    return results;
}

bool MultiLayerIndex::hasAttribute(const QString &name) {
    assert(isOpen());
    if (m_inMemoryIndex->hasAttribute(name)) {
        return true;
    }
    return m_persistentIndex->hasAttribute(name);
}

QString MultiLayerIndex::getAttribute(const QString &name) {
    assert(isOpen());
    if (m_inMemoryIndex->hasAttribute(name)) {
        return m_inMemoryIndex->getAttribute(name);
    }
    return m_persistentIndex->getAttribute(name);
}

uint64_t MultiLayerIndex::insertToOplog(pb::Operation *operation) {
    assert(isOpen());

    char op = '.';
    switch (operation->type()) {
        case pb::Operation::INSERT_OR_UPDATE_DOCUMENT:
            op = 'I';
            break;
        case pb::Operation::DELETE_DOCUMENT:
            op = 'D';
            break;
        case pb::Operation::SET_ATTRIBUTE:
            op = 'A';
            break;
    }

    std::string data;
    operation->SerializeToString(&data);

    const auto id = ++m_lastOplogId;
    const auto ts = QDateTime::currentMSecsSinceEpoch();

    qDebug() << "Inserting operation" << op << " to oplog" << id << "at" << ts;

    QSqlQuery query(m_db);
    query.prepare("INSERT INTO oplog (id, ts, op, data) VALUES (?, ?, ?, ?)");
    query.bindValue(0, id);
    query.bindValue(1, ts);
    query.bindValue(2, op);
    query.bindValue(3, QLatin1String(data.data(), data.size()));
    query.exec();
    return id;
}

void MultiLayerIndex::serialize(const InsertOrUpdateDocument &details, pb::Operation *op) {
    op->set_type(pb::Operation::INSERT_OR_UPDATE_DOCUMENT);
    auto d = op->insert_or_update_document_data();
    d.set_id(details.docId);
    auto terms = d.mutable_terms();
    terms->Reserve(details.terms.size());
    for (auto term : details.terms) {
        terms->AddAlreadyReserved(term);
    }
}

void MultiLayerIndex::serialize(const DeleteDocument &details, pb::Operation *op) {
    op->set_type(pb::Operation::DELETE_DOCUMENT);
    auto d = op->delete_document_data();
    d.set_id(details.docId);
}

void MultiLayerIndex::serialize(const SetAttribute &details, pb::Operation *op) {
    op->set_type(pb::Operation::SET_ATTRIBUTE);
    auto d = op->set_attribute_data();
    d.set_name(details.name.toStdString());
    d.set_value(details.value.toStdString());
}

void MultiLayerIndex::flush() {
    assert(isOpen());

    auto maxBatchSize = 10000;
    auto lastPersistedOplogId = m_lastPersistedOplogId;

    QSqlQuery q(m_db);
    q.prepare("SELECT id, ts, op, data FROM oplog WHERE id > ? ORDER BY id");
    q.bindValue(0, lastPersistedOplogId);
    q.exec();

    OpBatch batch;

    while (q.next()) {
        const auto id = q.value(0).toULongLong();
        const auto ts = q.value(1).toULongLong();
        const auto data = q.value(3).toByteArray();
        pb::Operation op;
        op.ParseFromArray(data.data(), data.size());
        switch (op.type()) {
            case pb::Operation::INSERT_OR_UPDATE_DOCUMENT: {
                auto d = op.insert_or_update_document_data();
                QVector<uint32_t> terms(d.terms().size());
                std::copy(d.terms().begin(), d.terms().end(), terms.begin());
                batch.insertOrUpdateDocument(d.id(), terms);
                break;
            }
            case pb::Operation::DELETE_DOCUMENT: {
                auto d = op.delete_document_data();
                batch.deleteDocument(d.id());
                break;
            }
            case pb::Operation::SET_ATTRIBUTE: {
                auto d = op.set_attribute_data();
                batch.setAttribute(QString::fromStdString(d.name()), QString::fromStdString(d.value()));
                break;
            }
        }
        lastPersistedOplogId = id;
        if (batch.size() > maxBatchSize) {
            m_persistentIndex->applyUpdates(batch);
            m_lastPersistedOplogId = lastPersistedOplogId;
            batch.clear();
        }
    }

    if (batch.size() > 0) {
        m_persistentIndex->applyUpdates(batch);
        m_lastPersistedOplogId = lastPersistedOplogId;
    }
}

void MultiLayerIndex::applyUpdates(const OpBatch &batch) {
    assert(isOpen());
    qDebug() << "applyUpdates";

    m_db.transaction();
    try {
        for (auto op : batch) {
            assert(op.isValid());
            pb::Operation entry;
            switch (op.type()) {
                case INSERT_OR_UPDATE_DOCUMENT:
                    serialize(std::get<InsertOrUpdateDocument>(op.data()), &entry);
                    break;
                case DELETE_DOCUMENT:
                    serialize(std::get<DeleteDocument>(op.data()), &entry);
                    break;
                case SET_ATTRIBUTE:
                    serialize(std::get<SetAttribute>(op.data()), &entry);
                    break;
            }
            insertToOplog(&entry);
        }
    } catch (...) {
        m_db.rollback();
        throw;
    }
    qDebug() << "Committing txn" << m_lastOplogId;
    m_db.commit();

    m_inMemoryIndex->applyUpdates(batch);
}

void MultiLayerIndex::setAttribute(const QString &name, const QString &value) {
    OpBatch batch;
    batch.setAttribute(name, value);
    applyUpdates(batch);
}

}  // namespace Acoustid
