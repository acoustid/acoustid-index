// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_BASE_INDEX_H_
#define ACOUSTID_INDEX_BASE_INDEX_H_

#include <QString>
#include <QVector>

#include "collector.h"

namespace Acoustid {

class BaseIndexTransaction {
 public:
    BaseIndexTransaction() {}
    virtual ~BaseIndexTransaction() {}

    virtual bool insertOrUpdateDocument(uint32_t docId, const QVector<uint32_t> &terms) = 0;
    virtual bool deleteDocument(uint32_t docId);

    virtual bool hasAttribute(const QString &name) = 0;
    virtual QString getAttribute(const QString &name) = 0;
    virtual void setAttribute(const QString &name, const QString &value) = 0;

    virtual void commit() = 0;
};

class BaseIndex {
 public:
    BaseIndex() {}
    virtual ~BaseIndex() {}
    
	virtual void search(const QVector<uint32_t> &terms, Collector *collector, int64_t timeoutInMSecs) = 0;

    virtual bool hasAttribute(const QString &name) = 0;
    virtual QString getAttribute(const QString &name) = 0;

//    QSharedPointer<BaseIndexTransaction> update() = 0
};

} // namespace Acoustid

#endif // ACOUSTID_INDEX_BASE_INDEX_H_
