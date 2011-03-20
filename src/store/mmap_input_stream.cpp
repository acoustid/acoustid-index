// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

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

