// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

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

