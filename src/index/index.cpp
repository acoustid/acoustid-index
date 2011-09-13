// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "store/directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "segment_index_reader.h"
#include "segment_data_reader.h"
#include "segment_searcher.h"
#include "index_file_deleter.h"
#include "index_reader.h"
#include "index_writer.h"
#include "index.h"

using namespace Acoustid;

Index::Index(Directory *dir)
	: m_mutex(QMutex::Recursive), m_dir(dir), m_open(false), m_indexWriter(NULL)
{
	m_deleter.reset(new IndexFileDeleter(m_dir));
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
	m_deleter->incRef(m_info);
	m_indexes = loadSegmentIndexes(m_dir, m_info);
	m_open = true;
}

void Index::refresh(const IndexInfo& info, const SegmentIndexMap& oldIndexes)
{
	SegmentIndexMap indexes = loadSegmentIndexes(m_dir, info, oldIndexes.isEmpty() ? m_indexes : oldIndexes);
	QMutexLocker locker(&m_mutex);
	if (m_open) {
		// the infos are opened twice (index + writer), so we need to inc/dec-ref them twice too
		m_deleter->incRef(info);
		m_deleter->incRef(info);
		m_deleter->decRef(m_info);
		m_deleter->decRef(m_info);
	}
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
	if (m_open) {
		m_deleter->incRef(m_info);
	}
	return new IndexReader(m_dir, m_info, m_indexes, this);
}

IndexWriter* Index::createWriter()
{
	QMutexLocker locker(&m_mutex);;
	if (m_indexWriter) {
		throw IOException("there already is an index writer open");
	}
	if (m_open) {
		m_deleter->incRef(m_info);
	}
	m_indexWriter = new IndexWriter(m_dir, m_info, m_indexes, this);
	return m_indexWriter;
}

void Index::onReaderDeleted(IndexReader* reader)
{
	QMutexLocker locker(&m_mutex);;
	qDebug() << "Reader deleted" << reader;
	if (m_open) {
		m_deleter->decRef(reader->info());
	}
}

void Index::onWriterDeleted(IndexWriter* writer)
{
	QMutexLocker locker(&m_mutex);;
	qDebug() << "Writer deleted" << writer;
	m_indexWriter = NULL;
}

