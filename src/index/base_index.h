// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_BASE_INDEX_H_
#define ACOUSTID_INDEX_BASE_INDEX_H_

#include "collector.h"

namespace Acoustid {

class BaseIndexTransaction {
 public:
    BaseIndexTransaction() {}
    virtual ~BaseIndexTransaction() {}

    virtual void insert(uint32_t docid, const uint32_t *hashes, size_t length) = 0;

    virtual QString getAttribute(const QString &name) = 0;
    virtual void setAttribute(const QString &name, const QString &value) = 0;

    virtual void commit() = 0;
};

class BaseIndex {
 public:
    BaseIndex() {}
    virtual ~BaseIndex() {}
    
	virtual void search(const uint32_t *fingerprint, size_t length, Collector *collector, int64_t timeoutInMSecs) = 0;

    virtual QString getAttribute(const QString &name) = 0;

//    QSharedPointer<BaseIndexTransaction> update() = 0
};

} // namespace Acoustid

#endif // ACOUSTID_INDEX_BASE_INDEX_H_
