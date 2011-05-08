// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include <QFile>
#include "util/test_utils.h"
#include "store/fs_input_stream.h"
#include "store/fs_output_stream.h"
#include "segment_index_writer.h"

using namespace Acoustid;

class SegmentIndexWriterTest : public ::testing::Test
{
protected:
	void SetUp()
	{
		stream = NamedFSOutputStream::openTemporary(true);
	}
	NamedFSOutputStream *stream;
};

TEST_F(SegmentIndexWriterTest, Write)
{
	SegmentIndexWriter writer(stream);
	writer.setBlockSize(256);
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

