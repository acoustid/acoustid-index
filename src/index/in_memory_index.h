// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_IN_MEMORY_INDEX_H_
#define ACOUSTID_INDEX_IN_MEMORY_INDEX_H_

#include <QHash>
#include <QMultiHash>
#include <QReadWriteLock>
#include <QString>
#include <QVector>

#include "base_index.h"
#include "collector.h"

namespace Acoustid {

struct InMemoryIndexData {
    QReadWriteLock lock;
    QHash<uint32_t, bool> docs;
    QMultiHash<uint32_t, uint32_t> index;
    QHash<QString, QString> attributes;

    void insertInternal(uint32_t docId, const QVector<uint32_t> &terms);
    bool deleteInternal(uint32_t docId);
};

class InMemoryIndex : public BaseIndex {
 public:
    InMemoryIndex();
    virtual ~InMemoryIndex() override;

    void reset();

    // Inserts or updates a document in the index. Returns true if the document
    // was updated, false if it was inserted.
    bool insertOrUpdateDocument(uint32_t docId, const QVector<uint32_t> &terms);

    // Removes a document from the index. Returns true if the document was
    // removed.
    bool deleteDocument(uint32_t docId);

    // Returns true if the index contains the specified document.
    bool containsDocument(uint32_t docId);

    // Returns true if the specified document has been deleted.
    bool isDocumentDeleted(uint32_t docId);

    virtual QVector<SearchResult> search(const QVector<uint32_t> &terms, int64_t timeoutInMSecs = 0) override;

    virtual bool hasAttribute(const QString &name) override;
    virtual QString getAttribute(const QString &name) override;
    void setAttribute(const QString &name, const QString &value);

    virtual void applyUpdates(const OpBatch &batch) override;

 private:
    ACOUSTID_DISABLE_COPY(InMemoryIndex);

    void insertInternal(uint32_t docId, const QVector<uint32_t> &terms);
    bool deleteInternal(uint32_t docId);

    QSharedPointer<InMemoryIndexData> m_data;
};

}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_IN_MEMORY_INDEX_H_
