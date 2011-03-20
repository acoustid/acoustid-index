// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "output_stream.h"

using namespace Acoustid;

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

