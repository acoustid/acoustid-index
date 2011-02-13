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
