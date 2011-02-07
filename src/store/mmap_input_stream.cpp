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
#include <sys/mman.h>
#include "common.h"
#include "mmap_input_stream.h"

using namespace Acoustid;

MMapInputStream::MMapInputStream(const uint8_t *addr, size_t length)
	: MemoryInputStream(addr, length)
{
}

MMapInputStream *MMapInputStream::open(const QString &fileName)
{
	QByteArray encodedFileName = QFile::encodeName(fileName);
	int fd = ::open(encodedFileName.data(), O_RDONLY);
	struct stat sb;
	fstat(fd, &sb);
	void *addr = ::mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
	return new MMapInputStream(static_cast<const uint8_t *>(addr), sb.st_size);
}

