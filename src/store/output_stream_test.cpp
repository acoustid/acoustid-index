// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include "util/test_utils.h"
#include "output_stream.h"

using namespace Acoustid;

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

TEST(OutputStreamTest, WriteByte)
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

TEST(OutputStreamTest, WriteInt16)
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

TEST(OutputStreamTest, WriteInt32)
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

TEST(OutputStreamTest, WriteVInt32)
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

TEST(OutputStreamTest, WriteBytes)
{
	uint8_t data[100];
	SimpleOutputStream outputStream(data);

	uint8_t d[] = { 1, 2, 3, 4 };
	outputStream.reset();
	outputStream.writeBytes(d, 4);
	uint8_t expected[] = { 1, 2, 3, 4 };
	ASSERT_EQ(4, outputStream.position());
	ASSERT_INTARRAY_EQ(expected, data, outputStream.position());
}

TEST(OutputStreamTest, WriteString)
{
	uint8_t data[100];
	SimpleOutputStream outputStream(data);

	outputStream.reset();
	outputStream.writeString("test");
	uint8_t expected[] = { 4, 't', 'e', 's', 't' };
	ASSERT_EQ(5, outputStream.position());
	ASSERT_INTARRAY_EQ(expected, data, outputStream.position());
}

