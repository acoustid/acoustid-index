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
	RAMDirectory dir;
	Index index(&dir);
	index.open(true);

	ScopedPtr<IndexWriter> writer(index.createWriter());
	ASSERT_TRUE(dir.fileExists("info_0"));
	ASSERT_EQ(0, writer->info().revision());
	ASSERT_EQ(0, writer->info().segmentCount());

	uint32_t fp[] = { 7, 9, 12 };
	writer->addDocument(1, fp, 3);
	writer->commit();
	ASSERT_TRUE(dir.fileExists("info_1"));
	ASSERT_TRUE(dir.fileExists("segment_0.fii"));
	ASSERT_TRUE(dir.fileExists("segment_0.fid"));
	ASSERT_EQ(1, writer->info().revision());
	ASSERT_EQ(1, writer->info().segmentCount());
	ASSERT_EQ("segment_0", writer->info().segment(0).name());
	ASSERT_EQ(1, writer->info().segment(0).blockCount());

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
	Index index(&dir);
	index.open(true);

	ScopedPtr<IndexWriter> writer(index.createWriter());
	ASSERT_TRUE(dir.fileExists("info_0"));
	ASSERT_EQ(0, writer->info().revision());
	ASSERT_EQ(0, writer->info().segmentCount());

	writer->segmentMergePolicy()->setMaxMergeAtOnce(2);
	writer->segmentMergePolicy()->setMaxSegmentsPerTier(2);

	uint32_t fp[] = { 7, 9, 12 };
	writer->addDocument(1, fp, 3);
	writer->commit();
	ASSERT_EQ(1, writer->info().segmentCount());
	ASSERT_EQ(1, writer->info().segment(0).blockCount());
	writer.reset(index.createWriter());
	writer->segmentMergePolicy()->setMaxMergeAtOnce(2);
	writer->segmentMergePolicy()->setMaxSegmentsPerTier(2);
	writer->addDocument(2, fp, 3);
	writer->commit();
	ASSERT_EQ(2, writer->info().segmentCount());
	ASSERT_EQ(1, writer->info().segment(0).blockCount());
	ASSERT_EQ(1, writer->info().segment(1).blockCount());
	writer.reset(index.createWriter());
	writer->segmentMergePolicy()->setMaxMergeAtOnce(2);
	writer->segmentMergePolicy()->setMaxSegmentsPerTier(2);
	writer->addDocument(3, fp, 3);
	writer->commit();
	ASSERT_EQ(2, writer->info().segmentCount());
	ASSERT_EQ(1, writer->info().segment(0).blockCount());
	ASSERT_EQ(1, writer->info().segment(1).blockCount());
	writer.reset(index.createWriter());
	writer->segmentMergePolicy()->setMaxMergeAtOnce(2);
	writer->segmentMergePolicy()->setMaxSegmentsPerTier(2);
	writer->addDocument(4, fp, 3);
	writer->commit();
	ASSERT_EQ(2, writer->info().segmentCount());
	ASSERT_EQ(1, writer->info().segment(0).blockCount());
	ASSERT_EQ(1, writer->info().segment(1).blockCount());
	writer.reset(index.createWriter());
	writer->segmentMergePolicy()->setMaxMergeAtOnce(2);
	writer->segmentMergePolicy()->setMaxSegmentsPerTier(2);
	writer->addDocument(5, fp, 3);
	writer->commit();
	ASSERT_EQ(2, writer->info().segmentCount());
	ASSERT_EQ(1, writer->info().segment(0).blockCount());
	ASSERT_EQ(1, writer->info().segment(1).blockCount());
}

