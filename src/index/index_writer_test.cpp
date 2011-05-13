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

	IndexWriter writer(&dir);
	ASSERT_FALSE(dir.fileExists("info_0"));
	ASSERT_THROW(writer.open(), IOException);
	ASSERT_FALSE(dir.fileExists("info_0"));
}

TEST(IndexWriterTest, OpenEmptyCreate)
{
	RAMDirectory dir;

	ASSERT_FALSE(dir.fileExists("info_0"));
	IndexWriter writer(&dir);
	writer.open(true);
	ASSERT_TRUE(dir.fileExists("info_0"));
	ASSERT_EQ(0, writer.revision());
}

TEST(IndexWriterTest, AddDocument)
{
	RAMDirectory dir;
	IndexWriter writer(&dir);
	writer.open(true);
	ASSERT_TRUE(dir.fileExists("info_0"));
	ASSERT_EQ(0, writer.revision());
	ASSERT_EQ(0, writer.segmentInfos().segmentCount());

	uint32_t fp[] = { 7, 9, 12 };
	writer.addDocument(1, fp, 3);
	writer.commit();
	ASSERT_TRUE(dir.fileExists("info_1"));
	ASSERT_TRUE(dir.fileExists("segment_0.fii"));
	ASSERT_TRUE(dir.fileExists("segment_0.fid"));
	ASSERT_EQ(1, writer.revision());
	ASSERT_EQ(1, writer.segmentInfos().segmentCount());
	ASSERT_EQ("segment_0", writer.segmentInfos().segment(0).name());
	ASSERT_EQ(1, writer.segmentInfos().segment(0).blockCount());

	{
		ScopedPtr<InputStream> input(dir.openFile("segment_0.fii"));
		ASSERT_EQ(7, input->readInt32());
	}

	{
		ScopedPtr<InputStream> input(dir.openFile("segment_0.fid"));
		ASSERT_EQ(3, input->readInt16());
		ASSERT_EQ(1, input->readVInt32());
		ASSERT_EQ(2, input->readVInt32());
		ASSERT_EQ(1, input->readVInt32());
		ASSERT_EQ(3, input->readVInt32());
		ASSERT_EQ(1, input->readVInt32());
	}
}

TEST(IndexWriterTest, Merge)
{
	RAMDirectory dir;
	IndexWriter writer(&dir);
	writer.open(true);
	ASSERT_TRUE(dir.fileExists("info_0"));
	ASSERT_EQ(0, writer.revision());
	ASSERT_EQ(0, writer.segmentInfos().segmentCount());

	writer.segmentMergePolicy()->setMaxMergeAtOnce(2);
	writer.segmentMergePolicy()->setMaxSegmentsPerTier(2);

	uint32_t fp[] = { 7, 9, 12 };
	writer.addDocument(1, fp, 3);
	writer.commit();
	ASSERT_EQ(1, writer.segmentInfos().segmentCount());
	ASSERT_EQ(1, writer.segmentInfos().segment(0).blockCount());
	writer.addDocument(2, fp, 3);
	writer.commit();
	ASSERT_EQ(2, writer.segmentInfos().segmentCount());
	ASSERT_EQ(1, writer.segmentInfos().segment(0).blockCount());
	ASSERT_EQ(1, writer.segmentInfos().segment(1).blockCount());
	writer.addDocument(3, fp, 3);
	writer.commit();
	ASSERT_EQ(2, writer.segmentInfos().segmentCount());
	ASSERT_EQ(1, writer.segmentInfos().segment(0).blockCount());
	ASSERT_EQ(1, writer.segmentInfos().segment(1).blockCount());
	writer.addDocument(4, fp, 3);
	writer.commit();
	ASSERT_EQ(2, writer.segmentInfos().segmentCount());
	ASSERT_EQ(1, writer.segmentInfos().segment(0).blockCount());
	ASSERT_EQ(1, writer.segmentInfos().segment(1).blockCount());
	writer.addDocument(5, fp, 3);
	writer.commit();
	ASSERT_EQ(2, writer.segmentInfos().segmentCount());
	ASSERT_EQ(1, writer.segmentInfos().segment(0).blockCount());
	ASSERT_EQ(1, writer.segmentInfos().segment(1).blockCount());
}

