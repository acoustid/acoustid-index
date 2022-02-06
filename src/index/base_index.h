// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_BASE_INDEX_H_
#define ACOUSTID_INDEX_BASE_INDEX_H_

#include <QJsonObject>
#include <QString>
#include <variant>
#include <vector>

#include "op.h"
#include "search_result.h"

namespace Acoustid {

class BaseIndex {
 public:
    BaseIndex() {}
    virtual ~BaseIndex() {}

    virtual bool containsDocument(uint32_t docId) = 0;
    virtual std::vector<SearchResult> search(const std::vector<uint32_t> &terms, int64_t timeoutInMSecs) = 0;

    virtual bool hasAttribute(const QString &name) = 0;
    virtual QString getAttribute(const QString &name) = 0;

    virtual void applyUpdates(const OpBatch &ops) = 0;
};

}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_BASE_INDEX_H_
