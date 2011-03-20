// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "buffered_input_stream.h"

using namespace Acoustid;

BufferedInputStream::BufferedInputStream(size_t bufferSize)
	: m_bufferSize(bufferSize), m_buffer(0), m_start(0), m_position(0), m_length(0)
{
}

BufferedInputStream::~BufferedInputStream()
{
}

size_t BufferedInputStream::bufferSize()
{
	return m_bufferSize;
}

void BufferedInputStream::setBufferSize(size_t bufferSize)
{
	m_bufferSize = bufferSize;
	m_buffer.reset(0);
	m_start += m_position;
	m_position = 0;
	m_length = 0;
}

uint8_t BufferedInputStream::readByte()
{
	if (m_position >= m_length) {
		refill();
	}
	return m_buffer[m_position++];
}

void BufferedInputStream::refill()
{
	m_start += m_position;
	m_position = 0;
	if (!m_buffer) {
		m_buffer.reset(new uint8_t[m_bufferSize]);
	}
	m_length = read(m_buffer.get(), m_start, m_bufferSize);
}

size_t BufferedInputStream::position()
{
	return m_start + m_position;
}

void BufferedInputStream::seek(size_t position)
{
	if (m_start <= position && position < (m_start + m_length)) {
		m_position = position - m_start;
	}
	else {
		m_start = position;
		m_position = 0;
		m_length = 0;
	}
}

