// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_H_
#define ACOUSTID_INDEX_H_

#include <QMutex>
#include <QWaitCondition>
#include <QDeadlineTimer>
#include "common.h"
#include "index.h"
#include "index_info.h"
#include "store/directory.h"
#include "segment_index.h"

namespace Acoustid {

class IndexFileDeleter;
class IndexReader;
class IndexWriter;

// Class for working with an on-disk index.
//
// This class is thread-safe and is intended to be shared by multiple
// threads. Threads can use it to open their own searchers or writers.
class Index : public QEnableSharedFromThis<Index>
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

    QSharedPointer<IndexReader> openReader();
    QSharedPointer<IndexWriter> openWriter(bool wait = false, int64_t timeoutInMSecs = 0);

	void acquireWriterLock(bool wait = false, int64_t timeoutInMSecs = 0);
	void releaseWriterLock();

	IndexInfo acquireInfo();
	void releaseInfo(const IndexInfo& info);
	void updateInfo(const IndexInfo& oldInfo, const IndexInfo& newInfo, bool updateIndex = false);

private:
	ACOUSTID_DISABLE_COPY(Index);

	void acquireWriterLockInt(bool wait, int64_t timeoutInMSecs);

	void open(bool create);

	QMutex m_mutex;
	DirectorySharedPtr m_dir;
	bool m_hasWriter;
    QWaitCondition m_writerReleased;
	std::unique_ptr<IndexFileDeleter> m_deleter;
	IndexInfo m_info;
	bool m_open;
};

typedef QWeakPointer<Index> IndexWeakPtr;
typedef QSharedPointer<Index> IndexSharedPtr;

}

#endif
