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

#include <gtest/gtest.h>
#include "input_stream.h"

class SimpleInputStream : public InputStream
{
public:
	SimpleInputStream(uint8_t *data) : m_data(data) { }

	uint8_t readByte()
	{
		return *m_data++;
	}

	size_t position()
	{
		return 0;
	}

	void seek(size_t)
	{
	}

private:
	uint8_t *m_data;
};

TEST(InputStream, ReadByte)
{
	uint8_t data[] = { 0, 0xff, 0x01 };
	SimpleInputStream inputStream(data);
	ASSERT_EQ(0x00, inputStream.readByte());
	ASSERT_EQ(0xff, inputStream.readByte());
	ASSERT_EQ(0x01, inputStream.readByte());
}

TEST(InputStream, ReadInt16)
{
	uint8_t data[] = { 0, 0, 0xff, 0xff, 0x01, 0x02 };
	SimpleInputStream inputStream(data);
	ASSERT_EQ(0x0000, inputStream.readInt16());
	ASSERT_EQ(0xffff, inputStream.readInt16());
	ASSERT_EQ(0x0102, inputStream.readInt16());
}

TEST(InputStream, ReadInt32)
{
	uint8_t data[] = { 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0x01, 0x02, 0x03, 0x04 };
	SimpleInputStream inputStream(data);
	ASSERT_EQ(0x00000000, inputStream.readInt32());
	ASSERT_EQ(0xffffffff, inputStream.readInt32());
	ASSERT_EQ(0x01020304, inputStream.readInt32());
}

TEST(InputStream, ReadVInt32)
{
	uint8_t data[] = {
		1,
		0x80 | 1, 2,
		0x80 | 1, 0x80 | 2, 3,
		0x80 | 1, 0x80 | 2, 0x80 | 3, 4,
		0x80 | 1, 0x80 | 2, 0x80 | 3, 0x80 | 4, 5,
	};
	SimpleInputStream inputStream(data);
	ASSERT_EQ(1, inputStream.readVInt32());
	ASSERT_EQ((2 << 7) | 1, inputStream.readVInt32());
	ASSERT_EQ((3 << 14) | (2 << 7) | 1, inputStream.readVInt32());
	ASSERT_EQ((4 << 21) | (3 << 14) | (2 << 7) | 1, inputStream.readVInt32());
	ASSERT_EQ((5 << 28) | (4 << 21) | (3 << 14) | (2 << 7) | 1, inputStream.readVInt32());
}

