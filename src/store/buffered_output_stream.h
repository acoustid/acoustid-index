// Acoustid Index -- Inverted index for audio fingerprints
// Copyright (C) 2011  Lukas Lalinsky
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

#ifndef ACOUSTID_BUFFERED_OUTPUT_STREAM_H_
#define ACOUSTID_BUFFERED_OUTPUT_STREAM_H_

#include "common.h"
#include "output_stream.h"

class BufferedOutputStream : public OutputStream 
{
public:
	BufferedOutputStream(size_t bufferSize = 1024 * 8);
	~BufferedOutputStream();

	size_t bufferSize();
	void setBufferSize(size_t size);

	void writeByte(uint8_t);
	void writeBytes(const uint8_t *data, size_t length);

	size_t position();
	void seek(size_t position);
	void flush();

protected:
	virtual size_t write(const uint8_t *data, size_t offset, size_t length) = 0;
	void flushBuffer();
	void refill();

private:
	ScopedArrayPtr<uint8_t> m_buffer;
	size_t m_bufferSize;
	size_t m_start;
	size_t m_position;
	size_t m_length;
};

#endif
