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
#include "util/test_utils.h"
#include "output_stream.h"

class SimpleOutputStream : public OutputStream
{
public:
	SimpleOutputStream(uint8_t *data) : m_data(data), m_length(0) { }

	void reset()
	{
		m_length = 0;
	}

	void writeByte(uint8_t b)
	{
		m_data[m_length++] = b;
	}

	size_t position()
	{
		return m_length;
	}

	void seek(size_t)
	{
	}

private:
	size_t m_length;
	uint8_t *m_data;
};

TEST(OutputStream, WriteByte)
{
	uint8_t data[100];
	uint8_t expected[] = { 1, 255 };
	SimpleOutputStream outputStream(data);

	outputStream.reset();
	outputStream.writeByte(1);
	ASSERT_EQ(1, outputStream.position());
	ASSERT_INTARRAY_EQ(&expected[0], data, outputStream.position());

	outputStream.reset();
	outputStream.writeByte(255);
	ASSERT_EQ(1, outputStream.position());
	ASSERT_INTARRAY_EQ(&expected[1], data, outputStream.position());
}

TEST(OutputStream, WriteInt16)
{
	uint8_t data[100];
	uint8_t expected[] = { 0, 1, /**/ 1, 0, /**/ 255, 255 };
	SimpleOutputStream outputStream(data);

	outputStream.reset();
	outputStream.writeInt16(1);
	ASSERT_EQ(2, outputStream.position());
	ASSERT_INTARRAY_EQ(&expected[0], data, outputStream.position());

	outputStream.reset();
	outputStream.writeInt16(256);
	ASSERT_EQ(2, outputStream.position());
	ASSERT_INTARRAY_EQ(&expected[2], data, outputStream.position());

	outputStream.reset();
	outputStream.writeInt16(0xffff);
	ASSERT_EQ(2, outputStream.position());
	ASSERT_INTARRAY_EQ(&expected[4], data, outputStream.position());
}

TEST(OutputStream, WriteInt32)
{
	uint8_t data[100];
	uint8_t expected[] = { 0, 0, 0, 1, /**/ 0, 0, 1, 0, /**/ 0, 0, 255, 255,
		255, 255, 255, 255 };
	SimpleOutputStream outputStream(data);

	outputStream.reset();
	outputStream.writeInt32(1);
	ASSERT_EQ(4, outputStream.position());
	ASSERT_INTARRAY_EQ(&expected[0], data, outputStream.position());

	outputStream.reset();
	outputStream.writeInt32(256);
	ASSERT_EQ(4, outputStream.position());
	ASSERT_INTARRAY_EQ(&expected[4], data, outputStream.position());

	outputStream.reset();
	outputStream.writeInt32(0xffff);
	ASSERT_EQ(4, outputStream.position());
	ASSERT_INTARRAY_EQ(&expected[8], data, outputStream.position());

	outputStream.reset();
	outputStream.writeInt32(0xffffffff);
	ASSERT_EQ(4, outputStream.position());
	ASSERT_INTARRAY_EQ(&expected[12], data, outputStream.position());
}

TEST(OutputStream, WriteVInt32)
{
	uint8_t data[100];
	SimpleOutputStream outputStream(data);

	outputStream.reset();
	outputStream.writeVInt32(0);
	uint8_t expected_0[] = { 0 };
	ASSERT_EQ(1, outputStream.position());
	ASSERT_INTARRAY_EQ(expected_0, data, outputStream.position());

	outputStream.reset();
	outputStream.writeVInt32(1);
	uint8_t expected_1[] = { 1 };
	ASSERT_EQ(1, outputStream.position());
	ASSERT_INTARRAY_EQ(expected_1, data, outputStream.position());

	outputStream.reset();
	outputStream.writeVInt32(128);
	uint8_t expected_128[] = { 128, 1 };
	ASSERT_EQ(2, outputStream.position());
	ASSERT_INTARRAY_EQ(expected_128, data, outputStream.position());

	outputStream.reset();
	outputStream.writeVInt32(16385);
	uint8_t expected_16385[] = { 129, 128, 1 };
	ASSERT_EQ(3, outputStream.position());
	ASSERT_INTARRAY_EQ(expected_16385, data, outputStream.position());
}

