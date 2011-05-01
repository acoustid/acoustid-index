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

#define BLOCK_SIZE 512

using namespace Acoustid;

IndexReader::IndexReader(Directory *dir)
	: m_dir(dir), m_revision(-1)
{
}

void IndexReader::open()
{
	m_revision = SegmentInfoList::findCurrentRevision(m_dir);
	if (m_revision != -1) {
		m_infos.read(m_dir->openFile(SegmentInfoList::segmentsFileName(m_revision)));
	}
	else {
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
		index = SegmentIndexReader(m_dir->openFile(info.name() + ".fii")).read();
		m_indexes.insert(info.id(), index);
	}
	return index;
}

void IndexReader::closeSegmentIndex(int i)
{
	const SegmentInfo &info = m_infos.info(i);
	m_indexes.remove(info.id());
}

void IndexReader::search(uint32_t *fingerprint, size_t length, Collector *collector)
{
	for (int i = 0; i < m_infos.size(); i++) {
		const SegmentInfo &info = m_infos.info(i);
		SegmentDataReader *dataReader = new SegmentDataReader(m_dir->openFile(info.name() + ".fid"), BLOCK_SIZE);
		SegmentSearcher searcher(segmentIndex(i), dataReader);
		searcher.search(fingerprint, length, collector);
	}
}

