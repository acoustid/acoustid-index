// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <QString>
#include <QByteArray>
#include <QFile>
#include <QDir>
#include <QMutexLocker>
#include "common.h"
#include "fs_input_stream.h"
#include "fs_output_stream.h"
#include "fs_directory.h"

using namespace Acoustid;

FSDirectory::FSDirectory(const QString &path)
	: m_path(path)
{
}

FSDirectory::~FSDirectory()
{
}

void FSDirectory::close()
{
	QMutexLocker locker(&m_mutex);
}

OutputStream *FSDirectory::createFile(const QString &name)
{
	QMutexLocker locker(&m_mutex);
	QString path = filePath(name);
	return FSOutputStream::open(path);
}

InputStream *FSDirectory::openFile(const QString &name)
{
	QMutexLocker locker(&m_mutex);
	QString path = filePath(name);
	FSFileSharedPtr file = m_openInputFiles.value(path).toStrongRef();
	FSInputStream *input;
	if (file.isNull()) {
		m_openInputFiles.remove(path);
		input = FSInputStream::open(path);
		m_openInputFiles.insert(path, input->file());
	}
	else {
		input = new FSInputStream(file);
	}
	return input;
}

void FSDirectory::deleteFile(const QString &name)
{
	QMutexLocker locker(&m_mutex);
	QFile::remove(filePath(name));
}

void FSDirectory::renameFile(const QString &oldName, const QString &newName)
{
	QMutexLocker locker(&m_mutex);
	QFile::rename(filePath(oldName), filePath(newName));
}

QStringList FSDirectory::listFiles()
{
	QMutexLocker locker(&m_mutex);
	QDir dir(m_path);
	return dir.entryList(QStringList(), QDir::Files);
}

bool FSDirectory::fileExists(const QString &name)
{
	QMutexLocker locker(&m_mutex);
	QFile::exists(filePath(name));
}

