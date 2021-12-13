// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "index/oplog.h"

#include <QDateTime>
#include <QJsonDocument>
#include <QSqlQuery>
#include <QVariant>

namespace Acoustid {

OpLog::OpLog(QSqlDatabase db) : m_db(db) { createTables(); }

void OpLog::createTables() {
    QMutexLocker locker(&m_mutex);

    QSqlQuery query(m_db);
    query.exec(
        "CREATE TABLE IF NOT EXISTS oplog ("
        "id INTEGER PRIMARY KEY, "
        "ts INTEGER NOT NULL, "
        "data TEXT NOT NULL"
        ")");

    query.exec("SELECT max(id) FROM oplog");
    if (query.first()) {
        m_lastId = query.value(0).toULongLong();
    } else {
        m_lastId = 0;
    }
}

uint64_t OpLog::read(std::vector<OpLogEntry> &entries, int limit, uint64_t lastId) {
    QMutexLocker locker(&m_mutex);

    QSqlQuery query(m_db);
    query.prepare("SELECT id, data FROM oplog WHERE id > ? ORDER BY id LIMIT ?");
    query.bindValue(0, qulonglong(lastId));
    query.bindValue(1, limit);
    query.exec();
    while (query.next()) {
        auto id = uint64_t(query.value(0).toULongLong());
        auto op = Op::fromJson(QJsonDocument::fromJson(query.value(1).toByteArray()).object());
        entries.emplace_back(id, op);
        lastId = id;
    }
    return lastId;
}

void OpLog::write(const OpBatch &batch) {
    QMutexLocker locker(&m_mutex);

    QSqlQuery query(m_db);
    m_db.transaction();
    auto id = m_lastId;
    try {
        query.prepare(QStringLiteral("INSERT INTO oplog (id, ts, data) VALUES (?, ?, ?)"));
        for (const auto &op : batch) {
            query.bindValue(0, qulonglong(++id));
            query.bindValue(1, QDateTime::currentMSecsSinceEpoch());
            query.bindValue(2, QJsonDocument(op.toJson()).toJson(QJsonDocument::Compact));
            query.exec();
        }
    } catch (...) {
        m_db.rollback();
        throw;
    }
    m_db.commit();
    m_lastId = id;
}

}  // namespace Acoustid
