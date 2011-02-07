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
	stream->writeInt32(2);
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
	ASSERT_EQ(2, index->indexInterval());

	ASSERT_EQ(8, index->levelKeyCount(0));
	uint32_t expected0[] = { 2, 3, 4, 5, 6, 7, 8, 9 };
	ASSERT_INTARRAY_EQ(expected0, index->levelKeys(0), 8);

	ASSERT_EQ(4, index->levelKeyCount(1));
	uint32_t expected1[] = { 2, 4, 6, 8 };
	ASSERT_INTARRAY_EQ(expected1, index->levelKeys(1), 4);

	ASSERT_EQ(2, index->levelKeyCount(2));
	uint32_t expected2[] = { 2, 6 };
	ASSERT_INTARRAY_EQ(expected2, index->levelKeys(2), 2);

	delete index;
	delete reader;
	delete input;
}

