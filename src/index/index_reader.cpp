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
	if (!m_infos.load(m_dir)) {
		throw IOException("there is no index in the directory");
	}
}

IndexReader::~IndexReader()
{
}

SegmentIndexSharedPtr IndexReader::segmentIndex(int i)
{
	const SegmentInfo &info = m_infos.info(i);
	SegmentIndexSharedPtr index(m_indexes.value(info.id()));
	if (index.isNull()) {
		index = SegmentIndexReader(m_dir->openFile(info.indexFileName()), info.blockCount()).read();
		m_indexes.insert(info.id(), index);
	}
	return index;
}

SegmentDataReader *IndexReader::segmentDataReader(int i)
{
	const SegmentInfo &info = m_infos.info(i);
	return new SegmentDataReader(m_dir->openFile(info.dataFileName()), BLOCK_SIZE);
}

void IndexReader::closeSegmentIndex(int i)
{
	const SegmentInfo &info = m_infos.info(i);
	m_indexes.remove(info.id());
}

void IndexReader::search(uint32_t *fingerprint, size_t length, Collector *collector)
{
	std::sort(fingerprint, fingerprint + length);
	for (int i = 0; i < m_infos.size(); i++) {
		SegmentSearcher searcher(segmentIndex(i), segmentDataReader(i), m_infos.info(i).lastKey());
		searcher.search(fingerprint, length, collector);
	}
}

