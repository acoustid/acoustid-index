// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <algorithm>
#include "store/directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "segment_document_reader.h"
#include "segment_index_data_reader.h"
#include "segment_searcher.h"
#include "index.h"
#include "index_reader.h"

using namespace Acoustid;

IndexReader::IndexReader(DirectorySharedPtr dir, const IndexInfo& info)
	: m_dir(dir), m_info(info)
{
}

IndexReader::IndexReader(IndexSharedPtr index)
	: m_dir(index->directory()), m_index(index)
{
	m_info = m_index->acquireInfo();
}

IndexReader::~IndexReader()
{
	if (m_index) {
		m_index->releaseInfo(m_info);
	}
}

SegmentIndexDataReader* IndexReader::segmentIndexDataReader(const SegmentInfo& segment)
{
	return new SegmentIndexDataReader(m_dir->openFile(segment.indexDataFileName()), BLOCK_SIZE);
}

SegmentDocumentReader* IndexReader::segmentDocumentReader(const SegmentInfo& segment)
{
	return new SegmentDocumentReader(m_dir->openFile(segment.documentDataFileName()));
}

bool IndexReader::get(uint32_t id, Document *doc)
{
	const SegmentInfoList& segments = m_info.segments();
	for (int i = 0; i < segments.size(); i++) {
		const SegmentInfo& s = segments.at(i);
		size_t position;
		if (s.documentIndex()->findPosition(id, &position)) {
			segmentDocumentReader(s)->readDocument(position, doc);
			return true;
		}
	}

	return false;
}

void IndexReader::search(uint32_t* fingerprint, size_t length, Collector* collector)
{
	std::sort(fingerprint, fingerprint + length);
	const SegmentInfoList& segments = m_info.segments();
	for (int i = 0; i < segments.size(); i++) {
		const SegmentInfo& s = segments.at(i);
		SegmentSearcher searcher(s.index(), segmentIndexDataReader(s), s.lastKey());
		searcher.search(fingerprint, length, collector);
	}
}

