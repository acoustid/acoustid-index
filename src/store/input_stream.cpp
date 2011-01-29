#include "input_stream.h"

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
	while (b & 0x80) {
		b = readByte();
		i = (i << 7) | (b & 0x7f);
	}
	return i;
}

