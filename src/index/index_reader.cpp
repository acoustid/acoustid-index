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

SegmentIndexSharedPtr IndexReader::segmentIndex(const SegmentInfo& segment)
{
	QReadLocker locker(&m_indexLock);
	SegmentIndexSharedPtr index(m_indexes.value(segment.id()));
	if (!index.isNull()) {
		return index;
	}
	locker.unlock();
	QWriteLocker writeLocker(&m_indexLock);
	index = m_indexes.value(segment.id()); // we need to recheck again, now with a write lock
	if (!index.isNull()) {
		return index;
	}
	index = SegmentIndexReader(m_dir->openFile(segment.indexFileName()), segment.blockCount()).read();
	m_indexes.insert(segment.id(), index);
	return index;
}

void IndexReader::closeSegmentIndex(const SegmentInfo& segment)
{
	QWriteLocker locker(&m_indexLock);
	m_indexes.remove(segment.id());
}

SegmentDataReader *IndexReader::segmentDataReader(const SegmentInfo& segment)
{
	return new SegmentDataReader(m_dir->openFile(segment.dataFileName()), BLOCK_SIZE);
}

void IndexReader::search(uint32_t* fingerprint, size_t length, Collector* collector)
{
	const SegmentInfoList& segments = info().segments();
	std::sort(fingerprint, fingerprint + length);
	for (int i = 0; i < segments.size(); i++) {
		const SegmentInfo& s = segments.at(i);
		SegmentSearcher searcher(segmentIndex(s), segmentDataReader(s), s.lastKey());
		searcher.search(fingerprint, length, collector);
	}
}

