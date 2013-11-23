// Copyright (C) 2013  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "index_utils.h"
#include "segment_document_merger.h"

using namespace Acoustid;

SegmentDocumentMerger::SegmentDocumentMerger(SegmentDocumentWriter *writer)
	: m_writer(writer)
{
}

SegmentDocumentMerger::~SegmentDocumentMerger()
{
	qDeleteAll(m_readers);
}

size_t SegmentDocumentMerger::merge()
{
	QList<SegmentDocumentEnum *> readers(m_readers);
	QMutableListIterator<SegmentDocumentEnum *> iter(readers);
	while (iter.hasNext()) {
		SegmentDocumentEnum *reader = iter.next();
		if (!reader->next()) {
			iter.remove();
		}
	}

	uint32_t lastMinId = UINT32_MAX;
	while (!readers.isEmpty()) {
		size_t minIdIndex = 0;
		uint32_t minId = UINT32_MAX;
		Document minIdDocument;
		for (size_t i = 0; i < readers.size(); i++) {
			SegmentDocumentEnum *reader = readers[i];
			uint32_t id = reader->id();
			if (id < minId) {
				minId = id;
				minIdIndex = i;
				minIdDocument = reader->document();
			}
		}
		if (minId != lastMinId) {
			m_writer->addDocument(minId, minIdDocument);
			lastMinId = minId;
		}
		if (!readers[minIdIndex]->next()) {
			readers.removeAt(minIdIndex);
		}
	}

	m_writer->close();
	return m_writer->documentCount();
}

