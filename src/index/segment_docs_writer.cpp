// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "store/output_stream.h"
#include "segment_docs_writer.h"

namespace Acoustid {

SegmentDocsWriter::SegmentDocsWriter(OutputStream *output)
	: m_output(output)
{
}

SegmentDocsWriter::~SegmentDocsWriter()
{
	close();
}

void SegmentDocsWriter::write(SegmentDocs *docs)
{
    for (auto doc : *docs) {
        m_output->writeInt32(doc.docId());
        m_output->writeInt32(doc.version());
        m_output->writeByte(doc.isDeleted() ? 1 : 0);
    }
}

void SegmentDocsWriter::close()
{
    m_output->writeInt32(0);
	m_output->flush();
}

void writeSegmentDocs(OutputStream *output, SegmentDocs *docs)
{
    SegmentDocsWriter writer(output);
    writer.write(docs);
}

}  // namespace Acoustid
