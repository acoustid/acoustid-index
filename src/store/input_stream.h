// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_STORE_INPUT_STREAM_H_
#define ACOUSTID_STORE_INPUT_STREAM_H_

#include "common.h"

namespace Acoustid {

class InputStream {

public:

	virtual ~InputStream();

	virtual uint8_t readByte() = 0;
	virtual uint16_t readInt16();
	virtual uint32_t readInt32();
	virtual uint32_t readVInt32();
	virtual QString readString();

	virtual size_t position() = 0;
	virtual void seek(size_t position) = 0;

};

}

#endif
