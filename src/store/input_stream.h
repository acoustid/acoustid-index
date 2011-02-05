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

#ifndef ACOUSTID_UTIL_INPUT_STREAM_H_
#define ACOUSTID_UTIL_INPUT_STREAM_H_

#include "common.h"

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

#endif
