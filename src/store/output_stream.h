// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_STORE_OUTPUT_STREAM_H_
#define ACOUSTID_STORE_OUTPUT_STREAM_H_

#include "common.h"

namespace Acoustid {

class OutputStream {

public:
	virtual ~OutputStream();

	virtual void writeByte(uint8_t value) = 0;
	virtual void writeBytes(const uint8_t *data, size_t length);
	virtual void writeInt16(uint16_t value);
	virtual void writeInt32(uint32_t value);
	virtual void writeVInt32(uint32_t value);
	virtual void writeString(const QString &value);

	virtual size_t position() = 0;
	virtual void seek(size_t position) = 0;
	virtual void flush() {};

};

}

#endif



