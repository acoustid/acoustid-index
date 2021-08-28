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

class MultiLayerIndex : public BaseIndex {
  public:
    MultiLayerIndex();
    virtual ~MultiLayerIndex() override;

    bool isOpen();

    void open(QSharedPointer<Directory> dir, bool create = false);

    // Inserts or updates a document in the index. Returns true if the document was updated, false if it was inserted.
    bool insertOrUpdateDocument(uint32_t docId, const QVector<uint32_t> &terms);

    // Removes a document from the index. Returns true if the document was removed.
    bool deleteDocument(uint32_t docId);

    // Returns true if the index contains the specified document.
    bool containsDocument(uint32_t docId);

    virtual void search(const QVector<uint32_t> &terms, Collector *collector, int64_t timeoutInMSecs = 0) override;

    virtual bool hasAttribute(const QString &name) override;
    virtual QString getAttribute(const QString &name) override;
    void setAttribute(const QString &name, const QString &value);

  private:
    int getDatabaseSchemaVersion();
    void updateDatabaseSchemaVersion(int version);

    void upgradeDatabaseSchema();
    void upgradeDatabaseSchemaV1();

  private:
    QSqlDatabase m_db;
    QSharedPointer<Index> m_persistentIndex;
    QSharedPointer<InMemoryIndex> m_inMemoryIndex;
};

} // namespace Acoustid

#endif // ACOUSTID_INDEX_MULTI_LAYER_INDEX_H_

