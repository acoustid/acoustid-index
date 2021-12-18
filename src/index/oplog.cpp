// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "index/oplog.h"

#include <sqlite3.h>

#include <QDateTime>
#include <QDebug>
#include <QJsonDocument>
#include <QVariant>

#include "util/defer.h"

namespace Acoustid {

Oplog::Oplog(sqlite3 *db) : m_db(db) { createTables(); }

Oplog::~Oplog() {
    if (m_db) {
        sqlite3_close(m_db);
        m_db = nullptr;
    }
}

void Oplog::createReplicationSlotsTable() {
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(m_db,
                           "CREATE TABLE IF NOT EXISTS replication_slots (\n"
                           "    slot_name TEXT PRIMARY KEY,\n"
                           "    last_op_id INTEGER NOT NULL,\n"
                           "    last_op_time INTEGER NOT NULL\n"
                           ")",
                           -1, &stmt, nullptr) != SQLITE_OK) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
    defer { sqlite3_finalize(stmt); };
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
}

void Oplog::createOplogTable() {
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(m_db,
                           "CREATE TABLE IF NOT EXISTS oplog (\n"
                           "    op_id INTEGER PRIMARY KEY,\n"
                           "    op_time INTEGER NOT NULL,\n"
                           "    op_data TEXT NOT NULL\n"
                           ")",
                           -1, &stmt, nullptr) != SQLITE_OK) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
    defer { sqlite3_finalize(stmt); };
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
}

void Oplog::createTables() {
    QMutexLocker locker(&m_mutex);
    createReplicationSlotsTable();
    createOplogTable();
}

void Oplog::createReplicationSlot(const QString &slotName) {
    QMutexLocker locker(&m_mutex);
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(m_db, "INSERT INTO replication_slots (slot_name, last_op_id, last_op_time) VALUES (?, 0, 0)",
                           -1, &stmt, nullptr) != SQLITE_OK) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
    defer { sqlite3_finalize(stmt); };
    if (sqlite3_bind_text(stmt, 1, slotName.toUtf8().constData(), -1, SQLITE_TRANSIENT) != SQLITE_OK) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
    auto rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        if (rc == SQLITE_CONSTRAINT) {
            throw ReplicationSlotAlreadyExistsError(slotName);
        }
        throw OplogError(sqlite3_errmsg(m_db));
    }
}

void Oplog::deleteReplicationSlot(const QString &slotName) {
    QMutexLocker locker(&m_mutex);
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(m_db, "DELETE FROM replication_slots WHERE slot_name = ?", -1, &stmt, nullptr) !=
        SQLITE_OK) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
    defer { sqlite3_finalize(stmt); };
    if (sqlite3_bind_text(stmt, 1, slotName.toUtf8().constData(), -1, SQLITE_TRANSIENT) != SQLITE_OK) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
    auto changes = sqlite3_changes(m_db);
    if (changes == 0) {
        throw ReplicationSlotDoesNotExistError(slotName);
    }
}

void Oplog::updateReplicationSlot(const QString &slotName, int64_t lastOpId) {
    QMutexLocker locker(&m_mutex);
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(m_db,
                           "UPDATE replication_slots SET last_op_id = ?, last_op_time = (SELECT op_time FROM oplog "
                           "WHERE op_id = ?) WHERE slot_name = ?",
                           -1, &stmt, nullptr) != SQLITE_OK) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
    defer { sqlite3_finalize(stmt); };
    if (sqlite3_bind_int64(stmt, 1, lastOpId) != SQLITE_OK) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
    if (sqlite3_bind_int64(stmt, 2, lastOpId) != SQLITE_OK) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
    if (sqlite3_bind_text(stmt, 3, slotName.toUtf8().constData(), -1, SQLITE_TRANSIENT) != SQLITE_OK) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
    auto rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        if (rc == SQLITE_CONSTRAINT) {
            throw OpDoesNotExistError(lastOpId);
        }
        throw OplogError(sqlite3_errmsg(m_db));
    }
    auto changes = sqlite3_changes(m_db);
    if (changes == 0) {
        throw ReplicationSlotDoesNotExistError(slotName);
    }
}

void Oplog::createOrUpdateReplicationSlot(const QString &slotName, int64_t lastOpId) {
    QMutexLocker locker(&m_mutex);
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(m_db,
                           "INSERT INTO replication_slots (slot_name, last_op_id, last_op_time) "
                           "VALUES (?, ?, COALESCE((SELECT op_time FROM oplog WHERE op_id = ?), 0)) "
                           "ON CONFLICT(slot_name) DO "
                           "UPDATE SET last_op_id=excluded.last_op_id, last_op_time=excluded.last_op_time",
                            -1, &stmt, nullptr) != SQLITE_OK) {
    }
    defer { sqlite3_finalize(stmt); };
    if (sqlite3_bind_text(stmt, 1, slotName.toUtf8().constData(), -1, SQLITE_TRANSIENT) != SQLITE_OK) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
    if (sqlite3_bind_int64(stmt, 2, lastOpId) != SQLITE_OK) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
    if (sqlite3_bind_int64(stmt, 3, lastOpId) != SQLITE_OK) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
}

int64_t Oplog::getLastOpId() {
    QMutexLocker locker(&m_mutex);
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(m_db, "SELECT MAX(op_id) FROM oplog", -1, &stmt, nullptr) != SQLITE_OK) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
    defer { sqlite3_finalize(stmt); };
    if (sqlite3_step(stmt) != SQLITE_ROW) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
    return sqlite3_column_int64(stmt, 0);
}

int64_t Oplog::getLastOpId(const QString &slotName) {
    QMutexLocker locker(&m_mutex);
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(m_db, "SELECT last_op_id FROM replication_slots WHERE slot_name = ?", -1, &stmt, nullptr) !=
        SQLITE_OK) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
    defer { sqlite3_finalize(stmt); };
    if (sqlite3_bind_text(stmt, 1, slotName.toUtf8().constData(), -1, SQLITE_TRANSIENT) != SQLITE_OK) {
        throw OplogError(sqlite3_errmsg(m_db));
    }
    auto rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        if (rc == SQLITE_DONE) {
            throw ReplicationSlotDoesNotExistError(slotName);
        }
        throw OplogError(sqlite3_errmsg(m_db));
    }
    return sqlite3_column_int64(stmt, 0);
}

int64_t Oplog::read(std::vector<OplogEntry> &entries, int limit, int64_t lastId) {
    QMutexLocker locker(&m_mutex);

    qDebug() << "Reading oplog entries from" << lastId;

    int rc;
    const auto selectSql = "SELECT op_id, op_data FROM oplog WHERE op_id > ? ORDER BY op_id LIMIT ?";

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
        auto id = sqlite3_column_int64(stmt, 0);
        auto opJson =
            QByteArray(reinterpret_cast<const char *>(sqlite3_column_blob(stmt, 1)), sqlite3_column_bytes(stmt, 1));
        auto op = Op::fromJson(QJsonDocument::fromJson(opJson).object());
        entries.emplace_back(id, op);
        lastId = id;
    }
    return lastId;
}

int64_t Oplog::write(const OpBatch &batch) {
    QMutexLocker locker(&m_mutex);

    int rc;
    const auto insertSql = "INSERT INTO oplog (op_time, op_data) VALUES (?, ?)";

    sqlite3_stmt *stmt = nullptr;
    rc = sqlite3_prepare_v2(m_db, insertSql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        throw Exception(QString("failed to prepare statement: %1").arg(sqlite3_errstr(rc)));
    }
    defer { sqlite3_finalize(stmt); };

    int64_t lastId;

    for (const auto &op : batch) {
        rc = sqlite3_bind_int64(stmt, 1, QDateTime::currentMSecsSinceEpoch());
        if (rc != SQLITE_OK) {
            throw Exception(QString("failed to bind parameter: %1").arg(sqlite3_errstr(rc)));
        }

        auto opJson = QJsonDocument(op.toJson()).toJson(QJsonDocument::Compact);
        rc = sqlite3_bind_blob(stmt, 2, opJson.data(), opJson.size(), SQLITE_STATIC);
        if (rc != SQLITE_OK) {
            throw Exception(QString("failed to bind parameter: %1").arg(sqlite3_errstr(rc)));
        }

        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            throw Exception(QString("failed to execute statement: %1").arg(sqlite3_errstr(rc)));
        }

        lastId = sqlite3_last_insert_rowid(m_db);

        rc = sqlite3_reset(stmt);
        if (rc != SQLITE_OK) {
            throw Exception(QString("failed to reset statement: %1").arg(sqlite3_errstr(rc)));
        }
    }

    return lastId;
}

}  // namespace Acoustid
