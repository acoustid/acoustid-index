// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_FILE_DELETER_H_
#define ACOUSTID_INDEX_FILE_DELETER_H_

#include "common.h"
#include "index_info.h"
#include "segment_info.h"

namespace Acoustid {

class IndexFileDeleter {
 public:
    IndexFileDeleter(DirectorySharedPtr dir);
    virtual ~IndexFileDeleter();

    void incRef(const IndexInfo& info);
    void decRef(const IndexInfo& info);
    void incRef(const SegmentInfo& info);
    void decRef(const SegmentInfo& info);
    void incRef(const QString& file);
    void decRef(const QString& file);

 protected:
    ACOUSTID_DISABLE_COPY(IndexFileDeleter);

    DirectorySharedPtr m_dir;
    QMap<QString, int> m_refCounts;
};

}  // namespace Acoustid

#endif
