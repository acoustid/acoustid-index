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

	uint8_t readByte();

	size_t position();
	void seek(size_t position);

protected:
	virtual size_t read(uint8_t *data, size_t offset, size_t length) = 0;
	void refill();

private:
	ScopedArrayPtr<uint8_t> m_buffer;
	size_t m_bufferSize;
	size_t m_start;
	size_t m_position;
	size_t m_length;
};

}

#endif
