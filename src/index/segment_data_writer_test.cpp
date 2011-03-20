// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include <QFile>
#include "util/test_utils.h"
#include "store/fs_input_stream.h"
#include "store/fs_output_stream.h"
#include "segment_data_writer.h"
#include "segment_index_writer.h"

using namespace Acoustid;

class SegmentDataWriterTest : public ::testing::Test
{
protected:
	void SetUp()
	{
		stream = NamedFSOutputStream::openTemporary();
		indexStream = NamedFSOutputStream::openTemporary();
	}
	void TearDown()
	{
		if (stream) {
			QFile::remove(stream->fileName());
			delete stream;
		}
		if (indexStream) {
			QFile::remove(indexStream->fileName());
			delete indexStream;
		}
	}
	NamedFSOutputStream *stream;
	NamedFSOutputStream *indexStream;
};

TEST_F(SegmentDataWriterTest, Write)
{
	SegmentIndexWriter indexWriter(indexStream);
	indexWriter.setBlockSize(8);

	SegmentDataWriter writer(stream, &indexWriter, indexWriter.blockSize());
	writer.addItem(200, 300);
	writer.addItem(201, 301);
	writer.addItem(201, 302);
	writer.addItem(202, 303);
	writer.close();

	ScopedPtr<FSInputStream> input(FSInputStream::open(stream->fileName()));

	ASSERT_EQ(3, input->readVInt32());
	ASSERT_EQ(300, input->readVInt32());
	ASSERT_EQ(1, input->readVInt32());
	ASSERT_EQ(301, input->readVInt32());
	ASSERT_EQ(0, input->readVInt32());
	ASSERT_EQ(1, input->readVInt32());
	ASSERT_EQ(1, input->readVInt32());
	ASSERT_EQ(303, input->readVInt32());
}

