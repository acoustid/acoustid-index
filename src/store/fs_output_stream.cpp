// Acoustid Index -- Inverted index for audio fingerprints
// Copyright (C) 2011  Lukas Lalinsky
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

#include <QString>
#include <QByteArray>
#include <QFile>
#include "common.h"
#include "fs_output_stream.h"

FSOutputStream::FSOutputStream(int fd)
	: m_fd(fd)
{
}

FSOutputStream::~FSOutputStream()
{
	flushBuffer(); // XXX this indirectly calls a virtual function write()
}

int FSOutputStream::fileDescriptor() const
{
	return m_fd;
}

size_t FSOutputStream::write(uint8_t *data, size_t offset, size_t length)
{
	ssize_t result = pwrite(m_fd, (void *)data, length, offset);
	if (result == -1) {
		return 0;
	}
	return result;
}

FSOutputStream *FSOutputStream::open(const QString &fileName)
{
	QByteArray encodedFileName = QFile::encodeName(fileName);
	int fd = ::open(encodedFileName.data(), O_WRONLY | O_CREAT);
	return new FSOutputStream(fd);
}

NamedFSOutputStream::NamedFSOutputStream(const QString &fileName, int fd)
	: FSOutputStream(fd), m_fileName(fileName)
{
}

QString NamedFSOutputStream::fileName() const
{
	return m_fileName;
}

NamedFSOutputStream *NamedFSOutputStream::openTemporary()
{
	QByteArray path("/tmp/acoustidXXXXXX");
	int fd = ::mkstemp(path.data());
	return new NamedFSOutputStream(path, fd);
}

