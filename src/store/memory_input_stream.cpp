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
#include "memory_input_stream.h"

MemoryInputStream::MemoryInputStream(const uint8_t *addr, size_t length)
	: m_addr(addr), m_length(length), m_position(0)
{
}

MemoryInputStream::~MemoryInputStream()
{
}

size_t MemoryInputStream::position()
{
	return m_position;
}

void MemoryInputStream::seek(size_t position)
{
	m_position = std::min(position, m_length);
}

uint8_t MemoryInputStream::readByte()
{
	return m_addr[m_position++];
}

uint32_t MemoryInputStream::readVInt32()
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

