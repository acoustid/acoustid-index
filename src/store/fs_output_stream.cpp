// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <QString>
#include <QByteArray>
#include <QFile>
#include <errno.h>
#include "common.h"
#include "fs_output_stream.h"

using namespace Acoustid;

FSOutputStream::FSOutputStream(const FSFileSharedPtr &file)
	: m_file(file)
{
}

FSOutputStream::~FSOutputStream()
{
	flushBuffer(); // XXX this indirectly calls a virtual function write()
}

int FSOutputStream::fileDescriptor() const
{
	return m_file->fileDescriptor();
}

size_t FSOutputStream::write(const uint8_t *data, size_t offset, size_t length)
{
	while (true) {
		ssize_t result = pwrite(fileDescriptor(), (void *)data, length, offset);
		if (result == -1) {
			if (errno == EINTR) {
				continue;
			}
			throw IOException(QString("Couldn't write to a file (errno %1)").arg(errno));
		}
		return result;
	}
}

FSOutputStream *FSOutputStream::open(const QString &fileName)
{
	QByteArray encodedFileName = QFile::encodeName(fileName);
	int fd = ::open(encodedFileName.data(), O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
	if (fd == -1) {
		throw IOException(QString("couldn't open the file '%1' for writing (errno %2)").arg(fileName).arg(errno));
	}
	return new FSOutputStream(FSFileSharedPtr(new FSFile(fd)));
}

NamedFSOutputStream::NamedFSOutputStream(const QString &fileName, const FSFileSharedPtr &file, bool autoDelete)
	: FSOutputStream(file), m_fileName(fileName), m_autoDelete(autoDelete)
{
}

NamedFSOutputStream::~NamedFSOutputStream()
{
	if (m_autoDelete) {
		QFile::remove(m_fileName);
	}
}

QString NamedFSOutputStream::fileName() const
{
	return m_fileName;
}

NamedFSOutputStream *NamedFSOutputStream::openTemporary(bool autoDelete)
{
	QByteArray path("/tmp/acoustidXXXXXX");
	int fd = ::mkstemp(path.data());
	if (fd == -1) {
		throw IOException("couldn't create a temporary file");
	}
	return new NamedFSOutputStream(path, FSFileSharedPtr(new FSFile(fd)), autoDelete);
}

