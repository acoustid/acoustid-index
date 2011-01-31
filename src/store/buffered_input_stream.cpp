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

#include "buffered_input_stream.h"

BufferedInputStream::BufferedInputStream(size_t bufferSize)
	: m_bufferSize(bufferSize), m_buffer(0), m_start(0), m_position(0), m_length(0)
{
}

BufferedInputStream::~BufferedInputStream()
{
	if (m_buffer) {
		delete[] m_buffer;
	}
}

size_t BufferedInputStream::bufferSize()
{
	return m_bufferSize;
}

void BufferedInputStream::setBufferSize(size_t bufferSize)
{
	m_bufferSize = bufferSize;
	if (m_buffer) {
		delete[] m_buffer;
		m_buffer = 0;
	}
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
		m_buffer = new uint8_t[m_bufferSize];
	}
	m_length = read(m_buffer, m_start, m_bufferSize);
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

