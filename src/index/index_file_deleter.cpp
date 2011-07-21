// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <QMutex>
#include "store/directory.h"
#include "index_file_deleter.h"

using namespace Acoustid;

IndexFileDeleter::IndexFileDeleter(Directory *dir)
	: m_mutex(QMutex::Recursive), m_dir(dir)
{
}

IndexFileDeleter::~IndexFileDeleter()
{
}

void IndexFileDeleter::incRef(const IndexInfo& info)
{
	QMutexLocker locker(&m_mutex);;
}

void IndexFileDeleter::decRef(const IndexInfo& info)
{
	QMutexLocker locker(&m_mutex);;
}

