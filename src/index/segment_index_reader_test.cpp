// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include <QFile>
#include "util/test_utils.h"
#include "store/fs_input_stream.h"
#include "store/fs_output_stream.h"
#include "segment_index.h"
#include "segment_index_reader.h"

using namespace Acoustid;

class SegmentIndexReaderTest : public ::testing::Test
{
protected:
	void SetUp()
	{
		stream = NamedFSOutputStream::openTemporary(true);
	}
    void TearDown()
	{
		if (stream) {
			delete stream;
		}
	}
	NamedFSOutputStream *stream;
};

TEST_F(SegmentIndexReaderTest, Read)
{
	stream->writeInt32(2);
	stream->writeInt32(3);
	stream->writeInt32(4);
	stream->writeInt32(5);
	stream->writeInt32(6);
	stream->writeInt32(7);
	stream->writeInt32(8);
	stream->writeInt32(9);
	stream->flush();

	FSInputStream *input = FSInputStream::open(stream->fileName());
	SegmentIndexSharedPtr index = SegmentIndexReader(input, 8).read();

	ASSERT_EQ(8, index->blockCount());
	uint32_t expected0[] = { 2, 3, 4, 5, 6, 7, 8, 9 };
	ASSERT_INTARRAY_EQ(expected0, index->keys(), 8);
}

