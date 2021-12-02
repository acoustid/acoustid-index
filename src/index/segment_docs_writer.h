// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_DOCS_WRITER_H_
#define ACOUSTID_INDEX_SEGMENT_DOCS_WRITER_H_

#include "common.h"
#include "segment_docs.h"

namespace Acoustid {

class OutputStream;

class SegmentDocsWriter {
 public:
    SegmentDocsWriter(OutputStream *output);
    virtual ~SegmentDocsWriter();

    void write(SegmentDocs *docs);
    void close();

 private:
    OutputStream *m_output;
};

void writeSegmentDocs(OutputStream *output, SegmentDocs *docs);

}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_SEGMENT_DOCS_WRITER_H_
