// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_MMAP_INPUT_STREAM_H_
#define ACOUSTID_MMAP_INPUT_STREAM_H_

#include "memory_input_stream.h"

namespace Acoustid {

class MMapInputStream : public MemoryInputStream
{
public:
	MMapInputStream(const uint8_t *addr, size_t m_length);

	static MMapInputStream *open(const QString &fileName);

private:
};

}

#endif

