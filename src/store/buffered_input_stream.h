// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_BUFFERED_INPUT_STREAM_H_
#define ACOUSTID_BUFFERED_INPUT_STREAM_H_

#include "common.h"
#include "input_stream.h"

namespace Acoustid {

class BufferedInputStream : public InputStream 
{
public:
	BufferedInputStream(size_t bufferSize = 1024);
	~BufferedInputStream();

	size_t bufferSize();
	void setBufferSize(size_t size);

	uint8_t readByte()
	{
		if (m_position >= m_length) {
			refill();
		}
		return m_buffer[m_position++];
	}

	uint32_t readVInt32();

	size_t position();
	void seek(size_t position);

protected:
	virtual size_t read(uint8_t *data, size_t offset, size_t length) = 0;
	void refill();

private:
	std::unique_ptr<uint8_t[]> m_buffer;
	size_t m_bufferSize;
	size_t m_start;
	size_t m_position;
	size_t m_length;
};

}

#endif
