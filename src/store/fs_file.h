// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_STORE_FS_FILE_H_
#define ACOUSTID_STORE_FS_FILE_H_

#include <QSharedPointer>
#include "common.h"

namespace Acoustid {

class FSFile
{
public:
	explicit FSFile(int fd) : m_fd(fd)
	{
	}

	~FSFile()
	{
		if (m_fd) {
			::close(m_fd);
		}
	}

	int fileDescriptor() const
	{
		return m_fd;
	}

private:
	int m_fd;
};

typedef QWeakPointer<FSFile> FSFileWeakPtr;
typedef QSharedPointer<FSFile> FSFileSharedPtr;

}

#endif
