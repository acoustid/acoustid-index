// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_H_
#define ACOUSTID_INDEX_H_

#include <QMutex>
#include "common.h"
#include "index_info.h"
#include "segment_index.h"

namespace Acoustid {

class IndexReader;
class IndexWriter;

// Class for working with an on-disk index.
//
// This class is thread-safe and is intended to be shared by multiple
// threads. Threads can use it to open their own searchers or writers.
class Index
{
public:
	// Build a new instance using the given directory
	Index(Directory *dir);
	virtual ~Index();

	// Open the index
	void open(bool create = false);

	// 
	void refresh(const IndexInfo& info, const SegmentIndexMap &oldIndexes = SegmentIndexMap());

	// Return the directory which contains the index data
	Directory *directory()
	{
		return m_dir;
	}

	// Create a new searcher
	IndexReader* createReader();

	// Create a new writer, there can be only one writer at a time
	IndexWriter* createWriter();

	// Load all segment indexes
	static SegmentIndexMap loadSegmentIndexes(Directory* dir, const IndexInfo& info, const SegmentIndexMap &oldIndexes = SegmentIndexMap());

private:
	ACOUSTID_DISABLE_COPY(Index);

	QMutex m_mutex;
	Directory *m_dir;
	IndexInfo m_info;
	SegmentIndexMap m_indexes;
	bool m_open;
};

}

#endif
