// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_H_
#define ACOUSTID_INDEX_H_

#include <QMutex>
#include "common.h"
#include "index.h"
#include "index_info.h"
#include "store/directory.h"
#include "segment_index.h"

namespace Acoustid {

class IndexFileDeleter;

// Class for working with an on-disk index.
//
// This class is thread-safe and is intended to be shared by multiple
// threads. Threads can use it to open their own searchers or writers.
class Index
{
public:
	// Build a new instance using the given directory
	Index(DirectorySharedPtr dir, bool create = false);
	virtual ~Index();

	// Return the directory which contains the index data
	DirectorySharedPtr directory()
	{
		return m_dir;
	}

	IndexInfo info()
	{
		return m_info;
	}

	void acquireWriterLock();
	void releaseWriterLock();

	IndexInfo acquireInfo();
	void releaseInfo(const IndexInfo& info);
	void updateInfo(const IndexInfo& oldInfo, const IndexInfo& newInfo, bool updateIndex = false);

private:
	ACOUSTID_DISABLE_COPY(Index);

	void open(bool create);

	QMutex m_mutex;
	DirectorySharedPtr m_dir;
	bool m_hasWriter;
	ScopedPtr<IndexFileDeleter> m_deleter;
	IndexInfo m_info;
	bool m_open;
};

typedef QWeakPointer<Index> IndexWeakPtr;
typedef QSharedPointer<Index> IndexSharedPtr;

}

#endif
