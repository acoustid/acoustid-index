// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "buffered_input_stream.h"

#include <gtest/gtest.h>

using namespace Acoustid;

class SimpleBufferedInputStream : public BufferedInputStream {
 public:
    SimpleBufferedInputStream(uint8_t *data) : m_data(data) {}

 protected:
    size_t read(uint8_t *data, size_t offset, size_t length) {
        memmove(data, m_data + offset, length);
        return length;
    }

 private:
    uint8_t *m_data;
};

TEST(BufferedInputStream, ReadByte) {
    uint8_t data[] = {0, 0xff, 0x01};
    SimpleBufferedInputStream inputStream(data);
    ASSERT_EQ(0x00, inputStream.readByte());
    ASSERT_EQ(0xff, inputStream.readByte());
    ASSERT_EQ(0x01, inputStream.readByte());
}

TEST(BufferedInputStreamTest, ReadInt16) {
    uint8_t data[] = {0, 0, 0xff, 0xff, 0x01, 0x02};
    SimpleBufferedInputStream inputStream(data);
    ASSERT_EQ(0x0000, inputStream.readInt16());
    ASSERT_EQ(0xffff, inputStream.readInt16());
    ASSERT_EQ(0x0102, inputStream.readInt16());
}

TEST(BufferedInputStreamTest, ReadInt32) {
    uint8_t data[] = {0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0x01, 0x02, 0x03, 0x04};
    SimpleBufferedInputStream inputStream(data);
    ASSERT_EQ(0x00000000, inputStream.readInt32());
    ASSERT_EQ(0xffffffff, inputStream.readInt32());
    ASSERT_EQ(0x01020304, inputStream.readInt32());
}

TEST(BufferedInputStreamTest, ReadVInt32) {
    uint8_t data[] = {
        1, 0x80 | 1, 2, 0x80 | 1, 0x80 | 2, 3, 0x80 | 1, 0x80 | 2, 0x80 | 3, 4, 0x80 | 1, 0x80 | 2, 0x80 | 3, 0x80 | 4, 5,
    };
    SimpleBufferedInputStream inputStream(data);
    ASSERT_EQ(1, inputStream.readVInt32());
    ASSERT_EQ((2 << 7) | 1, inputStream.readVInt32());
    ASSERT_EQ((3 << 14) | (2 << 7) | 1, inputStream.readVInt32());
    ASSERT_EQ((4 << 21) | (3 << 14) | (2 << 7) | 1, inputStream.readVInt32());
    ASSERT_EQ((5 << 28) | (4 << 21) | (3 << 14) | (2 << 7) | 1, inputStream.readVInt32());
}

TEST(BufferedInputStreamTest, ReadString) {
    uint8_t data[] = {4, 't', 'e', 's', 't'};
    SimpleBufferedInputStream inputStream(data);
    ASSERT_EQ(std::string("test"), inputStream.readString().toStdString());
}
