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

#ifndef ACOUSTID_OUTPUT_STREAM_H_
#define ACOUSTID_OUTPUT_STREAM_H_

#include "common.h"

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

#endif



