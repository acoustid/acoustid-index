// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <QString>
#include <QFile>
#include <errno.h>
#include "common.h"
#include "fs_input_stream.h"

using namespace Acoustid;

FSInputStream::FSInputStream(const FSFileSharedPtr &file)
	: m_file(file)
{
}

FSInputStream::~FSInputStream()
{
}

int FSInputStream::fileDescriptor() const
{
	return m_file->fileDescriptor();
}

const FSFileSharedPtr &FSInputStream::file() const
{
	return m_file;
}

size_t FSInputStream::read(uint8_t *data, size_t offset, size_t length)
{
	while (true) {
		ssize_t result = pread(fileDescriptor(), (void *)data, length, offset);
		if (result == -1) {
			if (errno == EINTR) {
				continue;
			}
			throw IOException(QString("Couldn't read from a file (errno %1)").arg(errno));
		}
		return result;
	}
}

FSInputStream *FSInputStream::open(const QString &fileName)
{
	QByteArray encodedFileName = QFile::encodeName(fileName);
	int fd = ::open(encodedFileName.data(), O_RDONLY);
	if (fd == -1) {
		throw IOException(QString("Couldn't open the file '%1' for reading (errno %2)").arg(fileName).arg(errno));
	}
	return new FSInputStream(FSFileSharedPtr(new FSFile(fd)));
}

