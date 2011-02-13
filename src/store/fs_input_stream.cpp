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
	ssize_t result = pread(fileDescriptor(), (void *)data, length, offset);
	if (result == -1) {
		throw IOException(QString("Couldn't read from a file (errno %1)").arg(errno));
	}
	return result;
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

