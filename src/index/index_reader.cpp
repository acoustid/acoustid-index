// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <algorithm>
#include "store/directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "segment_index_reader.h"
#include "segment_data_reader.h"
#include "segment_searcher.h"
#include "index.h"
#include "index_reader.h"

using namespace Acoustid;

IndexReader::IndexReader(DirectorySharedPtr dir, const IndexInfo& info, const SegmentIndexMap& indexes, Index* index)
	: m_dir(dir), m_info(info), m_indexes(indexes), m_index(index)
{
}

IndexReader::~IndexReader()
{
//	qDebug() << "IndexReader closed" << this << m_index;
	if (m_index) {
		m_index->onReaderDeleted(this);
	}
}

SegmentIndexSharedPtr IndexReader::segmentIndex(const SegmentInfo& segment)
{
	return m_indexes.value(segment.id());
}

SegmentDataReader* IndexReader::segmentDataReader(const SegmentInfo& segment)
{
	return new SegmentDataReader(m_dir->openFile(segment.dataFileName()), BLOCK_SIZE);
}

void IndexReader::search(uint32_t* fingerprint, size_t length, Collector* collector)
{
	std::sort(fingerprint, fingerprint + length);
	const SegmentInfoList& segments = m_info.segments();
	for (int i = 0; i < segments.size(); i++) {
		const SegmentInfo& s = segments.at(i);
		SegmentSearcher searcher(segmentIndex(s), segmentDataReader(s), s.lastKey());
		searcher.search(fingerprint, length, collector);
	}
}

