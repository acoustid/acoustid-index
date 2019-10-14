// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_CHECKSUM_OUTPUT_STREAM_H_
#define ACOUSTID_CHECKSUM_OUTPUT_STREAM_H_

#include "util/crc.h"
#include "output_stream.h"

namespace Acoustid {

class ChecksumOutputStream : public OutputStream
{
public:
	explicit ChecksumOutputStream(OutputStream *output);
	~ChecksumOutputStream();

	void writeByte(uint8_t b);

	uint32_t checksum();

	size_t position();
	void seek(size_t position);

private:
	ScopedPtr<OutputStream> m_output;
	crc_t m_crc;
};

}

#endif

