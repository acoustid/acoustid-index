// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_INDEX_OPLOG_H_
#define ACOUSTID_INDEX_INDEX_OPLOG_H_

#include <QMutex>
#include <QSqlDatabase>

#include "index/op.h"

namespace Acoustid {

class OpLogEntry {
 public:
    OpLogEntry(uint64_t id, const Op &op) : m_id(id), m_op(op) {}

    uint64_t id() const { return m_id; }
    const Op &op() const { return m_op; }

 private:
    uint64_t m_id;
    Op m_op;
};

class OpLog {
 public:
    explicit OpLog(QSqlDatabase db);

    uint64_t read(std::vector<OpLogEntry> &entries, int limit, uint64_t lastId = 0);
    void write(const OpBatch &batch);

 protected:
    void createTables();

 private:
    QSqlDatabase m_db;
    QMutex m_mutex;
    uint64_t m_lastId = 0;
};

}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_INDEX_OPLOG_H_
