// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "input_stream.h"

using namespace Acoustid;

InputStream::~InputStream()
{
}

uint16_t InputStream::readInt16()
{
	return (readByte() << 8) | readByte();
}

uint32_t InputStream::readInt32()
{
	return (readByte() << 24) | (readByte() << 16) | (readByte() << 8) | readByte();
}

uint32_t InputStream::readVInt32()
{
	uint8_t b = readByte();
	uint32_t i = b & 0x7f;
	int shift = 7;
	while (b & 0x80) {
		b = readByte();
		i |= (b & 0x7f) << shift;
		shift += 7;
	}
	return i;
}

QString InputStream::readString()
{
	size_t size = readVInt32();
	ScopedArrayPtr<uint8_t> data(new uint8_t[size]);
	for (size_t i = 0; i < size; i++) {
		data[i] = readByte();
	}
	return QString::fromUtf8(reinterpret_cast<const char *>(data.get()), size);
}

