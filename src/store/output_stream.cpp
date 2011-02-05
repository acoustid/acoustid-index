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

#include "output_stream.h"

OutputStream::~OutputStream()
{
}

void OutputStream::writeBytes(const uint8_t *data, size_t length)
{
	for (size_t i = 0; i < length; i++) {
		writeByte(data[i]);
	}
}

void OutputStream::writeInt16(uint16_t i)
{
	writeByte((i >>  8) & 0xff);
	writeByte((i      ) & 0xff);
}

void OutputStream::writeInt32(uint32_t i)
{
	writeByte((i >> 24) & 0xff);
	writeByte((i >> 16) & 0xff);
	writeByte((i >>  8) & 0xff);
	writeByte((i      ) & 0xff);
}

void OutputStream::writeVInt32(uint32_t i)
{
	while (i & ~0x7f) {
		writeByte((i & 0x7f) | 0x80);
		i >>= 7;
	}
	writeByte(i);
}

void OutputStream::writeString(const QString &s)
{
	QByteArray data = s.toUtf8();
	writeVInt32(data.size());
	writeBytes(reinterpret_cast<const uint8_t *>(data.constData()), data.size());
}

