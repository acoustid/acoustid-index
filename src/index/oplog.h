// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_INDEX_OPLOG_H_
#define ACOUSTID_INDEX_INDEX_OPLOG_H_

#include <QMutex>

#include "index/op.h"
#include "util/exceptions.h"

class sqlite3;

namespace Acoustid {

class OplogError : public Exception {
 public:
    OplogError(const QString &msg) : Exception(msg) {}
};

class OpDoesNotExistError : public OplogError {
 public:
    OpDoesNotExistError(int64_t opId) : OplogError(QStringLiteral("operation %1 does not exist").arg(opId)) {}
};

class ReplicationSlotDoesNotExistError : public OplogError {
 public:
    ReplicationSlotDoesNotExistError(const QString &slotName)
        : OplogError(QStringLiteral("replication slot '%1' does not exist").arg(slotName)) {}
};

class ReplicationSlotAlreadyExistsError : public OplogError {
 public:
    ReplicationSlotAlreadyExistsError(const QString &slotName)
        : OplogError(QStringLiteral("replication slot '%1' already exists").arg(slotName)) {}
};

class OplogEntry {
 public:
    OplogEntry(uint64_t id, const Op &op) : m_id(id), m_op(op) {}

    uint64_t id() const { return m_id; }
    const Op &op() const { return m_op; }

    bool operator==(const OplogEntry &other) const { return m_id == other.m_id && m_op == other.m_op; }

    bool operator!=(const OplogEntry &other) const { return !(*this == other); }

 private:
    uint64_t m_id;
    Op m_op;
};

class Oplog {
 public:
    explicit Oplog(sqlite3 *db);
    ~Oplog();

    void createReplicationSlot(const QString &slotName);
    void createOrUpdateReplicationSlot(const QString &slotName, int64_t lastOpId);
    void updateReplicationSlot(const QString &slotName, int64_t lastOpId);
    void deleteReplicationSlot(const QString &slotName);

    int64_t getLastOpId();
    int64_t getLastOpId(const QString &slotName);

    int64_t read(std::vector<OplogEntry> &entries, int limit, int64_t lastOpId = 0);
    int64_t write(const OpBatch &batch);

 protected:
    void createTables();
    void createOplogTable();
    void createReplicationSlotsTable();

 private:
    QMutex m_mutex;
    sqlite3 *m_db;
    uint64_t m_lastId = 0;
};

}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_INDEX_OPLOG_H_
