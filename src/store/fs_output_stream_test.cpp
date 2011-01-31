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
#include <QFile>
#include "util/test_utils.h"
#include "fs_output_stream.h"

class FSOutputStreamTest : public ::testing::Test
{
protected:
	void SetUp()
	{
		stream = NamedFSOutputStream::openTemporary();
	}
	void TearDown()
	{
		if (stream) {
			QFile::remove(stream->fileName());
			delete stream;
		}
	}
	NamedFSOutputStream *stream;
};

TEST_F(FSOutputStreamTest, Write)
{
	stream->writeByte(0);
	stream->writeByte(1);
	stream->writeByte(2);
	stream->writeByte(3);
	stream->writeByte(4);
	stream->writeByte(5);
	stream->writeByte(6);
	stream->writeByte(7);
	stream->flush();

	QFile file(stream->fileName());
	ASSERT_EQ(8, file.size());
	file.open(QIODevice::ReadOnly);
	QByteArray data = file.readAll();
	char expected[] = { 0, 1, 2, 3, 4, 5, 6, 7 };
	ASSERT_INTARRAY_EQ(expected, data.data(), 8);
	file.close();
}

TEST_F(FSOutputStreamTest, WriteAndSmallBufferSize)
{
	stream->setBufferSize(8);
	stream->writeByte(0);
	stream->writeByte(1);
	stream->writeByte(2);
	stream->writeByte(3);
	stream->writeByte(4);
	stream->writeByte(5);
	stream->writeByte(6);
	stream->writeByte(7);
	stream->writeByte(8);

	QFile file(stream->fileName());

	ASSERT_EQ(8, file.size());
	file.open(QIODevice::ReadOnly);
	QByteArray data = file.readAll();
	char expected[] = { 0, 1, 2, 3, 4, 5, 6, 7 };
	ASSERT_INTARRAY_EQ(expected, data.data(), 8);
	file.close();

	stream->flush();
	ASSERT_EQ(9, file.size());
	file.open(QIODevice::ReadOnly);
	QByteArray data2 = file.readAll();
	char expected2[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8 };
	ASSERT_INTARRAY_EQ(expected2, data2.data(), 9);
	file.close();
}

TEST_F(FSOutputStreamTest, WriteAfterSeek)
{
	stream->writeByte(0);
	stream->writeByte(1);
	stream->writeByte(2);
	stream->writeByte(3);
	stream->writeByte(4);
	stream->writeByte(5);
	stream->writeByte(6);
	stream->writeByte(7);
	ASSERT_EQ(8, stream->position());
	stream->seek(0);
	ASSERT_EQ(0, stream->position());
	stream->writeByte(9);
	stream->seek(3);
	ASSERT_EQ(3, stream->position());
	stream->writeByte(10);
	ASSERT_EQ(4, stream->position());
	stream->flush();

	QFile file(stream->fileName());

	ASSERT_EQ(8, file.size());
	file.open(QIODevice::ReadOnly);
	QByteArray data = file.readAll();
	char expected[] = { 9, 1, 2, 10, 4, 5, 6, 7 };
	ASSERT_INTARRAY_EQ(expected, data.data(), 8);
	file.close();
}

