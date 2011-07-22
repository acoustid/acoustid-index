// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "store/directory.h"
#include "index_file_deleter.h"

using namespace Acoustid;

IndexFileDeleter::IndexFileDeleter(Directory *dir)
	: m_dir(dir)
{
}

IndexFileDeleter::~IndexFileDeleter()
{
}

void IndexFileDeleter::incRef(const IndexInfo& info)
{
	QList<QString> files = info.files();
	for (int i = 0; i < files.size(); i++) {
		incRef(files.at(i));
	}
}

void IndexFileDeleter::incRef(const QString& file)
{
	m_refCounts[file] = m_refCounts.value(file) + 1;
}

void IndexFileDeleter::decRef(const IndexInfo& info)
{
	QList<QString> files = info.files();
	for (int i = 0; i < files.size(); i++) {
		decRef(files.at(i));
	}
}

void IndexFileDeleter::decRef(const QString& file)
{
	int count = m_refCounts.value(file) - 1;
	if (count <= 0) {
		qDebug() << "Deleting file" << file;
		m_dir->deleteFile(file);
		m_refCounts.remove(file);
	}
	else {
		m_refCounts[file] = count;
	}
}

