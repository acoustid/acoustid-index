// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <algorithm>
#include "store/directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "segment_index_reader.h"
#include "segment_data_reader.h"
#include "segment_searcher.h"
#include "index_reader.h"

using namespace Acoustid;

IndexReader::IndexReader(Directory *dir)
	: m_dir(dir)
{
}

void IndexReader::open()
{
	if (!m_info.load(m_dir)) {
		throw IOException("there is no index in the directory");
	}
}

IndexReader::~IndexReader()
{
}

SegmentIndexSharedPtr IndexReader::segmentIndex(int i)
{
	const SegmentInfo& segment = info().segment(i);
	SegmentIndexSharedPtr index(m_indexes.value(segment.id()));
	if (index.isNull()) {
		index = SegmentIndexReader(m_dir->openFile(segment.indexFileName()), segment.blockCount()).read();
		m_indexes.insert(segment.id(), index);
	}
	return index;
}

SegmentDataReader *IndexReader::segmentDataReader(int i)
{
	const SegmentInfo& segment = info().segment(i);
	return new SegmentDataReader(m_dir->openFile(segment.dataFileName()), BLOCK_SIZE);
}

void IndexReader::closeSegmentIndex(int i)
{
	const SegmentInfo& segment = info().segment(i);
	m_indexes.remove(segment.id());
}

void IndexReader::search(uint32_t *fingerprint, size_t length, Collector *collector)
{
	const SegmentInfoList& segments = info().segments();
	std::sort(fingerprint, fingerprint + length);
	for (int i = 0; i < segments.size(); i++) {
		SegmentSearcher searcher(segmentIndex(i), segmentDataReader(i), segments.at(i).lastKey());
		searcher.search(fingerprint, length, collector);
	}
}

