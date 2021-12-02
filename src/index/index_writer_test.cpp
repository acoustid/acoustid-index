// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include "util/test_utils.h"
#include "store/ram_directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "index_writer.h"
#include "index.h"

using namespace Acoustid;

TEST(IndexWriterTest, AddDocument)
{
	DirectorySharedPtr dir(new RAMDirectory());
	IndexSharedPtr index(new Index(dir, true));

	std::unique_ptr<IndexWriter> writer(new IndexWriter(index));
	ASSERT_TRUE(index->directory()->fileExists("info_1"));
	ASSERT_EQ(1, writer->info().revision());
	ASSERT_EQ(0, writer->info().segmentCount());
	ASSERT_EQ("", writer->info().attribute("max_document_id"));

    std::vector<uint32_t> fp { 7, 9, 12 };
	writer->insertOrUpdateDocument(1, fp);
	writer->commit();
	ASSERT_TRUE(index->directory()->fileExists("info_2"));
	ASSERT_TRUE(index->directory()->fileExists("segment_1.fii"));
	ASSERT_TRUE(index->directory()->fileExists("segment_1.fid"));
	ASSERT_TRUE(index->directory()->fileExists("segment_1.docs"));
	ASSERT_EQ(2, writer->info().revision());
	ASSERT_EQ(1, writer->info().segmentCount());
	ASSERT_EQ("1", writer->info().attribute("max_document_id"));
	ASSERT_EQ("segment_1", writer->info().segment(0).name());
	ASSERT_EQ(1, writer->info().segment(0).blockCount());
	ASSERT_EQ(3, writer->info().segment(0).checksum());

	{
		std::unique_ptr<InputStream> input(index->directory()->openFile("segment_1.fii"));
		ASSERT_EQ(7, input->readInt32());
	}

	{
		std::unique_ptr<InputStream> input(index->directory()->openFile("segment_1.fid"));
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
	DirectorySharedPtr dir(new RAMDirectory());
	IndexSharedPtr index(new Index(dir, true));

	std::unique_ptr<IndexWriter> writer(new IndexWriter(index));
	ASSERT_TRUE(index->directory()->fileExists("info_1"));
	ASSERT_EQ(1, writer->info().revision());
	ASSERT_EQ(0, writer->info().segmentCount());
	ASSERT_EQ(1, index->directory()->listFiles().size());

	writer->segmentMergePolicy()->setMaxMergeAtOnce(2);
	writer->segmentMergePolicy()->setMaxSegmentsPerTier(2);
	writer->segmentMergePolicy()->setFloorSegmentBlocks(0);

    std::vector<uint32_t> fp { 7, 9, 12 };
	writer->insertOrUpdateDocument(1, fp);
	writer->commit();
	ASSERT_EQ(1, writer->info().segmentCount());
	ASSERT_EQ(1, writer->info().segment(0).blockCount());
	writer.reset(NULL);
	writer.reset(new IndexWriter(index));
	ASSERT_EQ(4, index->directory()->listFiles().size());
	qDebug() << index->directory()->listFiles();
	writer->segmentMergePolicy()->setMaxMergeAtOnce(2);
	writer->segmentMergePolicy()->setMaxSegmentsPerTier(2);
	writer->segmentMergePolicy()->setFloorSegmentBlocks(0);
	writer->insertOrUpdateDocument(2, fp);
	writer->commit();
	ASSERT_EQ(2, writer->info().segmentCount());
	ASSERT_EQ(1, writer->info().segment(0).blockCount());
	ASSERT_EQ(1, writer->info().segment(1).blockCount());
	writer.reset(NULL);
	writer.reset(new IndexWriter(index));
	ASSERT_EQ(7, index->directory()->listFiles().size());
	qDebug() << index->directory()->listFiles();
	writer->segmentMergePolicy()->setMaxMergeAtOnce(2);
	writer->segmentMergePolicy()->setMaxSegmentsPerTier(2);
	writer->segmentMergePolicy()->setFloorSegmentBlocks(0);
	writer->insertOrUpdateDocument(3, fp);
	writer->commit();
	qDebug() << index->directory()->listFiles();
	ASSERT_EQ(2, writer->info().segmentCount());
	ASSERT_EQ(1, writer->info().segment(0).blockCount());
	ASSERT_EQ(1, writer->info().segment(1).blockCount());
	writer.reset(NULL);
	writer.reset(new IndexWriter(index));
	ASSERT_EQ(7, index->directory()->listFiles().size());
	qDebug() << index->directory()->listFiles();
	writer->segmentMergePolicy()->setMaxMergeAtOnce(2);
	writer->segmentMergePolicy()->setMaxSegmentsPerTier(2);
	writer->segmentMergePolicy()->setFloorSegmentBlocks(0);
	writer->insertOrUpdateDocument(4, fp);
	writer->commit();
	ASSERT_EQ(2, writer->info().segmentCount());
	ASSERT_EQ(1, writer->info().segment(0).blockCount());
	ASSERT_EQ(1, writer->info().segment(1).blockCount());
	writer.reset(NULL);
	writer.reset(new IndexWriter(index));
	ASSERT_EQ(7, index->directory()->listFiles().size());
	qDebug() << index->directory()->listFiles();
	writer->segmentMergePolicy()->setMaxMergeAtOnce(3);
	writer->segmentMergePolicy()->setMaxSegmentsPerTier(1);
	writer->segmentMergePolicy()->setFloorSegmentBlocks(0);
	writer->insertOrUpdateDocument(5, fp);
	writer->commit();
	ASSERT_EQ(1, writer->info().segmentCount());
	ASSERT_EQ(1, writer->info().segment(0).blockCount());
	writer.reset(NULL);
	ASSERT_EQ(4, index->directory()->listFiles().size());
	qDebug() << index->directory()->listFiles();
}

