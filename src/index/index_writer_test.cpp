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
#include "util/test_utils.h"
#include "store/ram_directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "index_writer.h"

using namespace Acoustid;

TEST(IndexWriterTest, OpenEmpty)
{
	RAMDirectory dir;

	ASSERT_FALSE(dir.fileExists("segments_0"));
	ASSERT_THROW(IndexWriter writer(&dir), IOException);
	ASSERT_FALSE(dir.fileExists("segments_0"));
}

TEST(IndexWriterTest, OpenEmptyCreate)
{
	RAMDirectory dir;

	ASSERT_FALSE(dir.fileExists("segments_0"));
	IndexWriter writer(&dir, true);
	ASSERT_TRUE(dir.fileExists("segments_0"));
	ASSERT_EQ(0, writer.revision());
}

TEST(IndexWriterTest, AddDocument)
{
	RAMDirectory dir;
	IndexWriter writer(&dir, true);
	ASSERT_TRUE(dir.fileExists("segments_0"));
	ASSERT_EQ(0, writer.revision());
	ASSERT_EQ(0, writer.segmentInfoList().segmentCount());

	uint32_t fp[] = { 7, 9, 12 };
	writer.addDocument(1, fp, 3);
	writer.commit();
	ASSERT_TRUE(dir.fileExists("segments_1"));
	ASSERT_TRUE(dir.fileExists("segment_0.fii"));
	ASSERT_TRUE(dir.fileExists("segment_0.fid"));
	ASSERT_EQ(1, writer.revision());
	ASSERT_EQ(1, writer.segmentInfoList().segmentCount());
	ASSERT_EQ("segment_0", writer.segmentInfoList().info(0).name());
	ASSERT_EQ(1, writer.segmentInfoList().info(0).numDocs());

	{
		ScopedPtr<InputStream> input(dir.openFile("segment_0.fii"));
		ASSERT_EQ(512, input->readInt32());
		ASSERT_EQ(32, input->readInt32());
		ASSERT_EQ(1, input->readInt32());
		ASSERT_EQ(7, input->readVInt32());
	}

	{
		ScopedPtr<InputStream> input(dir.openFile("segment_0.fid"));
		ASSERT_EQ(3, input->readVInt32());
		ASSERT_EQ(1, input->readVInt32());
		ASSERT_EQ(2, input->readVInt32());
		ASSERT_EQ(1, input->readVInt32());
		ASSERT_EQ(3, input->readVInt32());
		ASSERT_EQ(1, input->readVInt32());
	}
}

