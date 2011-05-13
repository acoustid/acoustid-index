// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_READER_H_
#define ACOUSTID_INDEX_READER_H_

#include <QReadLocker>
#include <QReadWriteLock>
#include <QWriteLocker>
#include "common.h"
#include "collector.h"
#include "segment_index.h"
#include "index_info.h"

namespace Acoustid {

class SegmentIndex;
class SegmentDataReader;

class IndexReader
{
public:
	IndexReader(Directory *dir);
	virtual ~IndexReader();

	void open();

	Directory *directory()
	{
		return m_dir;
	}

	const IndexInfo& info()
	{
		QReadLocker locker(&m_infoLock);
		return m_info;
	}

	void setInfo(const IndexInfo& info)
	{
		QWriteLocker locker(&m_infoLock);
		m_info = info;
	}

	void search(uint32_t *fingerprint, size_t length, Collector *collector);

protected:
	SegmentIndexSharedPtr segmentIndex(int i);
	void closeSegmentIndex(int i);
	SegmentDataReader *segmentDataReader(int i);

	QReadWriteLock m_infoLock;
	Directory *m_dir;
	QHash<int, SegmentIndexSharedPtr> m_indexes;
	IndexInfo m_info;
};

}

#endif
