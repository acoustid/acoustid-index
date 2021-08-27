// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_IN_MEMORY_INDEX_H_
#define ACOUSTID_INDEX_IN_MEMORY_INDEX_H_

#include <QHash>
#include <QString>
#include <QMutex>

#include "base_index.h"
#include "collector.h"

namespace Acoustid {

struct InMemoryIndexData {
    QMutex mutex;
    QHash<QString, QString> attributes;
};

class InMemoryIndex : public BaseIndex {
  public:
    InMemoryIndex();
    virtual ~InMemoryIndex() override;

    QString getAttribute(const QString &name) override;

  private:
    QSharedPointer<InMemoryIndexData> m_data;
};

} // namespace Acoustid

#endif // ACOUSTID_INDEX_IN_MEMORY_INDEX_H_
