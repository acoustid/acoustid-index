// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "store/directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "segment_index_reader.h"
#include "segment_data_reader.h"
#include "segment_searcher.h"
#include "index_reader.h"
#include "index_writer.h"
#include "index.h"

using namespace Acoustid;

Index::Index(Directory *dir)
	: m_mutex(QMutex::Recursive), m_dir(dir), m_open(false)
{
}

Index::~Index()
{
}

void Index::open(bool create)
{
	QMutexLocker locker(&m_mutex);
	if (!m_info.load(m_dir)) {
		if (create) {
			ScopedPtr<IndexWriter>(createWriter())->commit();
			return open(false);
	 	}
		throw IOException("there is no index in the directory");
	}
	m_indexes = loadSegmentIndexes(m_dir, m_info);
	m_open = true;
}

void Index::refresh(const IndexInfo& info)
{
	SegmentIndexMap indexes = loadSegmentIndexes(m_dir, info, m_indexes);
	QMutexLocker locker(&m_mutex);
	m_info = info;
	m_indexes = indexes;
}

SegmentIndexMap Index::loadSegmentIndexes(Directory* dir, const IndexInfo& info, const SegmentIndexMap& oldIndexes)
{
	SegmentIndexMap indexes;
	const SegmentInfoList& segments = info.segments();
	for (int i = 0; i < segments.size(); i++) {
		const SegmentInfo& segment = segments.at(i);
		SegmentIndexSharedPtr index = oldIndexes.value(segment.id());
		if (index.isNull()) {
			index = SegmentIndexReader(dir->openFile(segment.indexFileName()), segment.blockCount()).read();
		}
		indexes.insert(segment.id(), index);
	}
	return indexes;
}

IndexReader* Index::createReader()
{
	QMutexLocker locker(&m_mutex);;
	return new IndexReader(m_dir, m_info, m_indexes);
}

IndexWriter* Index::createWriter()
{
	QMutexLocker locker(&m_mutex);;
	return new IndexWriter(m_dir, m_info, m_indexes, this);
}

