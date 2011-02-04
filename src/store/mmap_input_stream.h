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

#ifndef ACOUSTID_MMAP_INPUT_STREAM_H_
#define ACOUSTID_MMAP_INPUT_STREAM_H_

#include "input_stream.h"

class MMapInputStream : public InputStream
{
public:
	explicit MMapInputStream(void *addr, size_t m_length);
	~MMapInputStream();

	size_t position();
	void seek(size_t position);

	uint8_t readByte();
	uint32_t readVInt32();

	static MMapInputStream *open(const QString &fileName);

private:
	uint8_t *m_addr;
	size_t m_length;
	size_t m_position;	
};

#endif

