// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_IN_MEMORY_INDEX_H_
#define ACOUSTID_INDEX_IN_MEMORY_INDEX_H_

#include <QHash>
#include <QString>
#include <QReadWriteLock>

#include "base_index.h"
#include "collector.h"

namespace Acoustid {

struct InMemoryIndexData {
    QReadWriteLock lock;
    QHash<uint32_t, uint32_t> index;
    QHash<QString, QString> attributes;
};

class InMemoryIndex : public BaseIndex {
  public:
    InMemoryIndex();
    virtual ~InMemoryIndex() override;

    virtual void search(const uint32_t *fingerprint, size_t length, Collector *collector, int64_t timeoutInMSecs) override;

    virtual bool hasAttribute(const QString &name) override;
    virtual QString getAttribute(const QString &name) override;

  private:
    QSharedPointer<InMemoryIndexData> m_data;
};

} // namespace Acoustid

#endif // ACOUSTID_INDEX_IN_MEMORY_INDEX_H_
