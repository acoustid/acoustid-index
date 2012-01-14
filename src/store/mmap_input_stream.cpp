// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <QString>
#include <QFile>
#include <errno.h>
#include <sys/mman.h>
#include "common.h"
#include "mmap_input_stream.h"

using namespace Acoustid;

MMapInputStream::MMapInputStream(const FSFileSharedPtr &file)
	: MemoryInputStream(file->mmapAddress(), file->mmapLength()), m_file(file)
{
}

int MMapInputStream::fileDescriptor() const
{
	return m_file->fileDescriptor();
}

const FSFileSharedPtr& MMapInputStream::file() const
{
	return m_file;
}

MMapInputStream *MMapInputStream::open(const QString &fileName)
{
	QByteArray encodedFileName = QFile::encodeName(fileName);
	int fd = ::open(encodedFileName.data(), O_RDONLY);
	if (fd == -1) {
		throw IOException(QString("Couldn't open the file '%1' for reading (errno %2)").arg(fileName).arg(errno));
	}
	struct stat sb;
	fstat(fd, &sb);
	void *addr = ::mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
	if (addr == MAP_FAILED) {
		::close(fd);
		throw IOException(QString("Couldn't map the file '%1' to memory (errno %2)").arg(fileName).arg(errno));
	}
	::madvise(addr, sb.st_size, MADV_RANDOM | MADV_WILLNEED);
	return new MMapInputStream(FSFileSharedPtr(new FSFile(fd, addr, sb.st_size)));
}

