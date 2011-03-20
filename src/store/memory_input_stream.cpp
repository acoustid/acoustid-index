// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <QString>
#include <QFile>
#include <sys/mman.h>
#include "common.h"
#include "memory_input_stream.h"

using namespace Acoustid;

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

