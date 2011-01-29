#define _XOPEN_SOURCE 500
#include <unistd.h>
#include <fcntl.h>
#include <QString>
#include <QFile>
#include "fs_input_stream.h"

FSInputStream::FSInputStream(int fd)
	: m_fd(fd)
{
}

FSInputStream::~FSInputStream()
{
}

int FSInputStream::fileDescriptor() const
{
	return m_fd;
}

size_t FSInputStream::read(uint8_t *data, size_t offset, size_t length)
{
	ssize_t result = pread(m_fd, (void *)data, length, offset);
	if (result == -1) {
		return 0;
	}
	return result;
}

FSInputStream *FSInputStream::open(const QString &fileName)
{
	QByteArray encodedFileName = QFile::encodeName(fileName);
	int fd = ::open(encodedFileName.data(), O_RDONLY);
	return new FSInputStream(fd);
}

