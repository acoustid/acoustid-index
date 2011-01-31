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
#include "store/fs_input_stream.h"
#include "store/fs_output_stream.h"
#include "segment_index_writer.h"

class SegmentIndexWriterTest : public ::testing::Test
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

TEST_F(SegmentIndexWriterTest, Write)
{
	SegmentIndexWriter writer(stream);
	writer.setBlockSize(256);
	writer.setIndexInterval(2);
	writer.addItem(2);
	writer.addItem(3);
	writer.addItem(4);
	writer.addItem(5);
	writer.addItem(6);
	writer.addItem(7);
	writer.addItem(8);
	writer.addItem(9);
	writer.close();

	FSInputStream *input = FSInputStream::open(stream->fileName());

	ASSERT_EQ(256, input->readInt32());
	ASSERT_EQ(2, input->readInt32());

	ASSERT_EQ(8, input->readInt32());
	ASSERT_EQ(2, input->readVInt32());
	ASSERT_EQ(1, input->readVInt32());
	ASSERT_EQ(1, input->readVInt32());
	ASSERT_EQ(1, input->readVInt32());
	ASSERT_EQ(1, input->readVInt32());
	ASSERT_EQ(1, input->readVInt32());
	ASSERT_EQ(1, input->readVInt32());
	ASSERT_EQ(1, input->readVInt32());

	delete input;
}

