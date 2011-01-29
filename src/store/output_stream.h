#ifndef ACOUSTID_OUTPUT_STREAM_H_
#define ACOUSTID_OUTPUT_STREAM_H_

#include <stdlib.h>
#include <stdint.h>

class OutputStream {

public:
	virtual ~OutputStream();

	virtual void writeByte(uint8_t value) = 0;
	virtual void writeInt16(uint16_t value) = 0;
	virtual void writeInt32(uint32_t value) = 0;
	virtual void writeVInt32(uint32_t value) = 0;

	virtual size_t position() = 0;
	virtual void seek(size_t position) = 0;
	virtual void flush() = 0;

};

#endif



