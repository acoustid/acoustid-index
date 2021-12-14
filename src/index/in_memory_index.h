// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_IN_MEMORY_INDEX_H_
#define ACOUSTID_INDEX_IN_MEMORY_INDEX_H_

#include <QHash>
#include <QMultiHash>
#include <QReadWriteLock>
#include <QString>
#include <memory>
#include <vector>

#include "base_index.h"

namespace Acoustid {

struct InMemoryIndexData;

class InMemoryIndex : public BaseIndex {
 public:
    InMemoryIndex();
    virtual ~InMemoryIndex() override;

    // Disable copying
    InMemoryIndex(const InMemoryIndex &) = delete;
    InMemoryIndex(InMemoryIndex &&) = delete;
    InMemoryIndex &operator=(const InMemoryIndex &) = delete;
    InMemoryIndex &operator=(InMemoryIndex &&) = delete;

    // Remove all data from the index
    void clear();

    size_t size();

    virtual bool containsDocument(uint32_t docId) override {
        bool isDeleted;
        return getDocument(docId, isDeleted) && !isDeleted;
    }

    bool getDocument(uint32_t docId, bool &isDeleted);

    virtual std::vector<SearchResult> search(const std::vector<uint32_t> &terms, int64_t timeoutInMSecs) override;

    virtual bool hasAttribute(const QString &name) override;
    virtual QString getAttribute(const QString &name) override;
    void setAttribute(const QString &name, const QString &value);

    virtual void applyUpdates(const OpBatch &batch) override;

 private:
    QReadWriteLock m_lock;
    std::unique_ptr<InMemoryIndexData> m_data;
};

}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_IN_MEMORY_INDEX_H_
