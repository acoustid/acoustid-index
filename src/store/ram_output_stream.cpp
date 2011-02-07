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

#include "common.h"
#include "ram_output_stream.h"

RAMOutputStream::RAMOutputStream(QByteArray *data)
	: m_buffer(data)
{
	m_buffer.open(QBuffer::WriteOnly);
}

RAMOutputStream::~RAMOutputStream()
{
}

void RAMOutputStream::writeByte(uint8_t b)
{
	m_buffer.write(reinterpret_cast<const char *>(&b), 1);
}

size_t RAMOutputStream::position()
{
	return m_buffer.pos();
}

void RAMOutputStream::seek(size_t position)
{
	m_buffer.seek(position);
}

