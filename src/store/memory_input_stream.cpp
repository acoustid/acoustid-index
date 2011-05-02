// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <QString>
#include <QFile>
#include <sys/mman.h>
#include "common.h"
#include "util/vint.h"
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
	if (m_length - m_position >= kMaxVInt32Bytes) {
		// We have enough data in the buffer for any vint32, so we can use an
		// optimized function for reading from memory array.
		uint32_t result;
		ssize_t size = readVInt32FromArray(&m_addr[m_position], &result);
		if (size == -1) {
			throw IOException("can't read vint32");
		}
		m_position += size;
		return result;
	}
	// We will probably run at the end of the stream, use the generic
	// implementation and let readByte() handle that.
	return InputStream::readVInt32();
}

