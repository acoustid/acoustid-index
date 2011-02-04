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

MMapInputStream::MMapInputStream(void *addr, size_t length)
	: m_addr(static_cast<uint8_t *>(addr)), m_length(length), m_position(0)
{
}

MMapInputStream::~MMapInputStream()
{
}

size_t MMapInputStream::position()
{
	return m_position;
}

void MMapInputStream::seek(size_t position)
{
	m_position = std::min(position, m_length);
}

uint8_t MMapInputStream::readByte()
{
	return m_addr[m_position++];
}

uint32_t MMapInputStream::readVInt32()
{
	uint8_t b = m_addr[m_position++];
	uint32_t i = b & 0x7f;
	int shift = 7;
	while (b & 0x80) {
		b = m_addr[m_position++];
		i |= (b & 0x7f) << shift;
		shift += 7;
	}
	return i;
}

MMapInputStream *MMapInputStream::open(const QString &fileName)
{
	QByteArray encodedFileName = QFile::encodeName(fileName);
	int fd = ::open(encodedFileName.data(), O_RDONLY);
	struct stat sb;
	fstat(fd, &sb);
	void *addr = ::mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
	return new MMapInputStream(addr, sb.st_size);
}

