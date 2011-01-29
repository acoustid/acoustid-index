#include "output_stream.h"

OutputStream::~OutputStream()
{
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

