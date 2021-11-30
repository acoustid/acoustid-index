// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_CHECKSUM_INPUT_STREAM_H_
#define ACOUSTID_CHECKSUM_INPUT_STREAM_H_

#include "util/crc.h"
#include "input_stream.h"

namespace Acoustid {

class ChecksumInputStream : public InputStream
{
public:
	explicit ChecksumInputStream(InputStream *input);
	~ChecksumInputStream();

	virtual uint8_t readByte();

	uint32_t checksum();

	size_t position();
	void seek(size_t position);

private:
	std::unique_ptr<InputStream> m_input;
	crc_t m_crc;
};

}

#endif

