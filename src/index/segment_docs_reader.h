// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEGMENT_DOCS_READER_H_
#define ACOUSTID_INDEX_SEGMENT_DOCS_READER_H_

#include "common.h"
#include "segment_docs.h"

namespace Acoustid {

class InputStream;

class SegmentDocsReader
{
public:
	SegmentDocsReader(InputStream *input);
    std::shared_ptr<SegmentDocs> read();

private:
	std::unique_ptr<InputStream> m_input;
};

}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_SEGMENT_DOCS_READER_H_
