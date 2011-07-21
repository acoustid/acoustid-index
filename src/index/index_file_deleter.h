// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_FILE_DELETER_H_
#define ACOUSTID_INDEX_FILE_DELETER_H_

#include "common.h"
#include "segment_index.h"
#include "index_info.h"

namespace Acoustid {

class IndexFileDeleter
{
public:
	IndexFileDeleter(Directory* dir);
	virtual ~IndexFileDeleter();

	void incRef(const IndexInfo& info);
	void decRef(const IndexInfo& info);

protected:
	ACOUSTID_DISABLE_COPY(IndexFileDeleter);

	QMutex m_mutex;
	Directory* m_dir;
	QMap<QString, int> m_refCounts;
};

}

#endif
