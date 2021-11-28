// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_MULTI_LAYER_INDEX_H_
#define ACOUSTID_INDEX_MULTI_LAYER_INDEX_H_

#include <QString>
#include <QVector>

#include "base_index.h"
#include "collector.h"
#include "index.h"
#include "in_memory_index.h"

namespace Acoustid {

namespace pb {
class Operation;
}

class MultiLayerIndex : public BaseIndex {
  public:
    MultiLayerIndex();
    virtual ~MultiLayerIndex() override;

    bool isOpen();

    void open(QSharedPointer<Directory> dir, bool create = false);

    virtual void search(const QVector<uint32_t> &terms, Collector *collector, int64_t timeoutInMSecs = 0) override;

    virtual bool hasAttribute(const QString &name) override;
    virtual QString getAttribute(const QString &name) override;

    void setAttribute(const QString &name, const QString &value);
    virtual void applyUpdates(const OpBatch &batch) override;

    void flush();

  private:
    int getDatabaseSchemaVersion();
    void updateDatabaseSchemaVersion(int version);

    void upgradeDatabaseSchema();
    void upgradeDatabaseSchemaV1();

    static void serialize(const InsertOrUpdateDocument &data, pb::Operation *op);
    static void serialize(const DeleteDocument &data, pb::Operation *op);
    static void serialize(const SetAttribute &data, pb::Operation *op);

    uint64_t insertToOplog(pb::Operation *op);

  private:
	ACOUSTID_DISABLE_COPY(MultiLayerIndex);

    QSqlDatabase m_db;
    QSharedPointer<Index> m_persistentIndex;
    QSharedPointer<InMemoryIndex> m_inMemoryIndex;
    quint64 m_lastPersistedOplogId = 0;
    quint64 m_lastOplogId = 0;
};

} // namespace Acoustid

#endif // ACOUSTID_INDEX_MULTI_LAYER_INDEX_H_
