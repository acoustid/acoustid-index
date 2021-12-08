// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_MULTI_INDEX_H_
#define ACOUSTID_INDEX_MULTI_INDEX_H_

#include <QMap>
#include <QSharedPointer>
#include <QString>

#include "index.h"
#include "store/directory.h"

namespace Acoustid {

class MultiIndex {
 public:
    MultiIndex(const QSharedPointer<Directory> &dir);

    QSharedPointer<Directory> dir() const { return m_dir; }

    bool indexExists(const QString &name);
    QSharedPointer<Index> getIndex(const QString &name, bool create = false);
    void createIndex(const QString &name);
    void deleteIndex(const QString &name);

 private:
    QMutex m_mutex;
    QSharedPointer<Directory> m_dir;
    QMap<QString, QSharedPointer<Index>> m_indexes;
};

}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_MULTI_INDEX_H_
