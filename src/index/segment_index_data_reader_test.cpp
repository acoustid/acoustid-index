// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include <QFile>
#include "util/test_utils.h"
#include "store/fs_input_stream.h"
#include "store/fs_output_stream.h"
#include "segment_index_data_reader.h"
#include "segment_index_data_writer.h"

using namespace Acoustid;

class SegmentIndexDataReaderTest : public ::testing::Test
{
protected:
	void SetUp()
	{
		stream = NamedFSOutputStream::openTemporary(true);
		indexStream = NamedFSOutputStream::openTemporary(true);
	}
	NamedFSOutputStream *stream;
	NamedFSOutputStream *indexStream;
};

TEST_F(SegmentIndexDataReaderTest, Read)
{
	stream->writeInt16(2);
	stream->writeVInt32(300);
	stream->writeVInt32(1);
	stream->writeVInt32(301);
	while (stream->position() < 8) {
		stream->writeByte(0);
	}

	stream->writeInt16(2);
	stream->writeVInt32(302);
	stream->writeVInt32(1);
	stream->writeVInt32(303);
	stream->flush();

	indexStream->writeInt32(200);
	indexStream->writeInt32(201);
	indexStream->flush();

	{
		SegmentIndexSharedPtr index = SegmentIndexDataReader::readIndex(FSInputStream::open(indexStream->fileName()), 2);
		ASSERT_EQ(2, index->blockCount());
		ASSERT_EQ(200, index->keys()[0]);
		ASSERT_EQ(201, index->keys()[1]);
	}

	{
		SegmentIndexDataReader reader(FSInputStream::open(stream->fileName()), 8);

		BlockDataIterator* iter = reader.readBlock(0, 200);
		ASSERT_EQ(2, iter->length());
		ASSERT_TRUE(iter->next());
		ASSERT_EQ(200, iter->key());
		ASSERT_EQ(300, iter->value());
		ASSERT_TRUE(iter->next());
		ASSERT_EQ(201, iter->key());
		ASSERT_EQ(301, iter->value());
		ASSERT_FALSE(iter->next());

		iter = reader.readBlock(1, 201);
		ASSERT_EQ(2, iter->length());
		ASSERT_TRUE(iter->next());
		ASSERT_EQ(201, iter->key());
		ASSERT_EQ(302, iter->value());
		ASSERT_TRUE(iter->next());
		ASSERT_EQ(202, iter->key());
		ASSERT_EQ(303, iter->value());
		ASSERT_FALSE(iter->next());
	}

}
