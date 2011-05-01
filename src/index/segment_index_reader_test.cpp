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

TEST_F(SegmentIndexReaderTest, Read)
{
	stream->writeInt32(256);
	stream->writeInt32(8);
	stream->writeVInt32(2);
	stream->writeVInt32(1);
	stream->writeVInt32(1);
	stream->writeVInt32(1);
	stream->writeVInt32(1);
	stream->writeVInt32(1);
	stream->writeVInt32(1);
	stream->writeVInt32(1);
	stream->flush();

	FSInputStream *input = FSInputStream::open(stream->fileName());
	SegmentIndexReader *reader = new SegmentIndexReader(input);
	SegmentIndex *index = reader->read();

	ASSERT_EQ(256, index->blockSize());

	ASSERT_EQ(8, index->levelKeyCount(0));
	uint32_t expected0[] = { 2, 3, 4, 5, 6, 7, 8, 9 };
	ASSERT_INTARRAY_EQ(expected0, index->levelKeys(0), 8);

	delete index;
	delete reader;
}

