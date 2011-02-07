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
	indexWriter.setIndexInterval(2);

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

