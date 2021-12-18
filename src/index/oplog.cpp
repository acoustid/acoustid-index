// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "index/oplog.h"

#include <QDateTime>
#include <QDebug>
#include <QJsonDocument>
#include <QVariant>

#include "util/defer.h"
#include "util/exceptions.h"

namespace Acoustid {

OpLog::OpLog(sqlite3 *db) : m_db(db) { createTables(); }

OpLog::~OpLog() {
    if (m_db) {
        sqlite3_close(m_db);
        m_db = nullptr;
    }
}

void OpLog::createTables() {
    QMutexLocker locker(&m_mutex);

    int rc;

    const auto createTableSql =
        "CREATE TABLE IF NOT EXISTS oplog (\n"
        "  id INTEGER PRIMARY KEY,\n"
        "  data TEXT NOT NULL\n"
        ")\n";

    sqlite3_stmt *stmt = nullptr;
    rc = sqlite3_prepare_v2(m_db, createTableSql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        throw Exception(QString("failed to prepare statement: %1").arg(sqlite3_errstr(rc)));
    }
    defer { sqlite3_finalize(stmt); };

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        throw Exception(QString("failed to execute statement: %1").arg(sqlite3_errstr(rc)));
    }
}

uint64_t OpLog::read(std::vector<OpLogEntry> &entries, int limit, uint64_t lastId) {
    QMutexLocker locker(&m_mutex);

    qDebug() << "Reading oplog entries from" << lastId;

    int rc;
    const auto selectSql = "SELECT id, data FROM oplog WHERE id > ? ORDER BY id LIMIT ?";

    sqlite3_stmt *stmt = nullptr;
    rc = sqlite3_prepare_v2(m_db, selectSql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        throw Exception(QString("failed to prepare statement: %1").arg(sqlite3_errstr(rc)));
    }
    defer { sqlite3_finalize(stmt); };

    rc = sqlite3_bind_int64(stmt, 1, lastId);
    if (rc != SQLITE_OK) {
        throw Exception(QString("failed to bind parameter: %1").arg(sqlite3_errstr(rc)));
    }

    rc = sqlite3_bind_int(stmt, 2, limit);
    if (rc != SQLITE_OK) {
        throw Exception(QString("failed to bind parameter: %1").arg(sqlite3_errstr(rc)));
    }

    while (true) {
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_ROW) {
            if (rc == SQLITE_DONE) {
                qDebug() << "No more oplog entries";
                break;
            }
            throw Exception(QString("failed to execute statement: %1").arg(sqlite3_errstr(rc)));
        }
        auto id = static_cast<uint64_t>(sqlite3_column_int64(stmt, 0));
        auto opJson = QByteArray(reinterpret_cast<const char *>(sqlite3_column_blob(stmt, 1)), sqlite3_column_bytes(stmt, 1));
        auto op = Op::fromJson(QJsonDocument::fromJson(opJson).object());
        entries.emplace_back(id, op);
        lastId = id;
    }
    return lastId;
}

uint64_t OpLog::write(const OpBatch &batch) {
    QMutexLocker locker(&m_mutex);

    int rc;
    const auto insertSql = "INSERT INTO oplog (data) VALUES (?)";

    sqlite3_stmt *stmt = nullptr;
    rc = sqlite3_prepare_v2(m_db, insertSql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        throw Exception(QString("failed to prepare statement: %1").arg(sqlite3_errstr(rc)));
    }
    defer { sqlite3_finalize(stmt); };

    uint64_t lastId;

    for (const auto &op : batch) {
        auto opJson = QJsonDocument(op.toJson()).toJson(QJsonDocument::Compact);
        rc = sqlite3_bind_blob(stmt, 1, opJson.data(), opJson.size(), SQLITE_STATIC);
        if (rc != SQLITE_OK) {
            throw Exception(QString("failed to bind parameter: %1").arg(sqlite3_errstr(rc)));
        }

        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            throw Exception(QString("failed to execute statement: %1").arg(sqlite3_errstr(rc)));
        }

        lastId = static_cast<uint64_t>(sqlite3_last_insert_rowid(m_db));

        rc = sqlite3_reset(stmt);
        if (rc != SQLITE_OK) {
            throw Exception(QString("failed to reset statement: %1").arg(sqlite3_errstr(rc)));
        }
    }

    return lastId;
}

}  // namespace Acoustid
