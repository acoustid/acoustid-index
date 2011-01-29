#ifndef ACOUSTID_INPUT_STREAM_H_
#define ACOUSTID_INPUT_STREAM_H_

#include <stdlib.h>
#include <stdint.h>

class InputStream {

public:

	virtual ~InputStream();

	virtual uint8_t readByte() = 0;
	virtual uint16_t readInt16();
	virtual uint32_t readInt32();
	virtual uint32_t readVInt32();

	virtual size_t position() = 0;
	virtual void seek(size_t position) = 0;

};

#endif
