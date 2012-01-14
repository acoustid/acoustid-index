// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_STORE_FS_FILE_H_
#define ACOUSTID_STORE_FS_FILE_H_

#include <QSharedPointer>
#include <sys/mman.h>
#include "common.h"

namespace Acoustid {

class FSFile
{
public:
	explicit FSFile(int fd, void* addr = NULL, size_t length = 0)
		: m_fd(fd), m_addr(addr), m_length(length)
	{
	}

	~FSFile()
	{
		if (m_addr) {
			::munmap(m_addr, m_length);
		}
		if (m_fd) {
			::close(m_fd);
		}
	}

	int fileDescriptor() const
	{
		return m_fd;
	}

	const uint8_t* mmapAddress() const
	{
		return static_cast<const uint8_t*>(m_addr);
	}

	size_t mmapLength() const
	{
		return m_length;
	}

private:
	int m_fd;
	void *m_addr;
	size_t m_length;
};

typedef QWeakPointer<FSFile> FSFileWeakPtr;
typedef QSharedPointer<FSFile> FSFileSharedPtr;

}

#endif
