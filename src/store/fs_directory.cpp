// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <errno.h>
#include <QString>
#include <QByteArray>
#include <QFile>
#include <QDir>
#include <QMutexLocker>
#include "common.h"
#include "mmap_input_stream.h"
#include "fs_input_stream.h"
#include "fs_output_stream.h"
#include "fs_directory.h"

using namespace Acoustid;

FSDirectory::FSDirectory(const QString &path, bool mmap)
	: m_path(path), m_mmap(mmap)
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
	FSFileSharedPtr file = m_openInputFiles.value(path);
	if (m_mmap) {
		if (file.isNull()) {
			MMapInputStream* input = MMapInputStream::open(path);
			m_openInputFiles.insert(path, input->file());
			return input;
		}
		return new MMapInputStream(file);
	}
	if (file.isNull()) {
		FSInputStream* input = FSInputStream::open(path);
		m_openInputFiles.insert(path, input->file());
		return input;
	}
	return new FSInputStream(file);
}

void FSDirectory::deleteFile(const QString &name)
{
	QMutexLocker locker(&m_mutex);
	QString path = filePath(name);
	m_openInputFiles.remove(path);
	QFile::remove(path);
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

void FSDirectory::sync(const QStringList& names)
{
	for (size_t i = 0; i < names.size(); i++) {
		fsync(names.at(i));
	}
}

void FSDirectory::fsync(const QString& name)
{
	QString fileName = filePath(name);
	ScopedPtr<FSInputStream> input(FSInputStream::open(fileName));
	int ret = ::fsync(input->fileDescriptor());
	if (ret == -1) {
		throw IOException(QString("Couldn't synchronize file '%1' (errno %2)").arg(fileName).arg(errno));
	}
}

