// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_H_
#define ACOUSTID_INDEX_H_

#include <QMutex>
#include "common.h"
#include "index_info.h"
#include "store/directory.h"
#include "segment_index.h"

namespace Acoustid {

class IndexReader;
class IndexWriter;
class IndexFileDeleter;

// Class for working with an on-disk index.
//
// This class is thread-safe and is intended to be shared by multiple
// threads. Threads can use it to open their own searchers or writers.
class Index
{
public:
	// Build a new instance using the given directory
	Index(DirectorySharedPtr dir);
	virtual ~Index();

	// Open the index
	void open(bool create = false);

	// 
	void refresh(const IndexInfo& info, const SegmentIndexMap &oldIndexes = SegmentIndexMap());

	// Return the directory which contains the index data
	DirectorySharedPtr directory()
	{
		return m_dir;
	}

	// Return the file deleted controlling this index's directory
	IndexFileDeleter* fileDeleter()
	{
		return m_deleter.get();
	}

	IndexInfo info()
	{
		return m_info;
	}

	// Create a new searcher
	IndexReader* createReader();

	// Create a new writer, there can be only one writer at a time
	IndexWriter* createWriter();

	// Load all segment indexes
	static SegmentIndexMap loadSegmentIndexes(Directory* dir, const IndexInfo& info, const SegmentIndexMap &oldIndexes = SegmentIndexMap());

	void onReaderDeleted(IndexReader* reader);
	void onWriterDeleted(IndexWriter* writer);

	void incFileRef(const SegmentInfo& segment);
	void decFileRef(const SegmentInfoList& segments);

private:
	ACOUSTID_DISABLE_COPY(Index);

	QMutex m_mutex;
	DirectorySharedPtr m_dir;
	IndexWriter* m_indexWriter;
	ScopedPtr<IndexFileDeleter> m_deleter;
	IndexInfo m_info;
	SegmentIndexMap m_indexes;
	bool m_open;
};

typedef QWeakPointer<Index> IndexWeakPtr;
typedef QSharedPointer<Index> IndexSharedPtr;

}

#endif
