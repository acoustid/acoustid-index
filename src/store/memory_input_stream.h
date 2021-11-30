// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_STORE_MEMORY_INPUT_STREAM_H_
#define ACOUSTID_STORE_MEMORY_INPUT_STREAM_H_

#include "input_stream.h"

namespace Acoustid {

class MemoryInputStream : public InputStream
{
public:
	explicit MemoryInputStream(const uint8_t *addr, size_t m_length);
	~MemoryInputStream();

	size_t position();
	void seek(size_t position);

	uint8_t readByte();
	uint32_t readVInt32();

private:
	const uint8_t *m_addr;
	size_t m_length;
	size_t m_position;	
};

}

#endif

