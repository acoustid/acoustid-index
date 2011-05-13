// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include "util/test_utils.h"
#include "store/ram_directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "top_hits_collector.h"
#include "index_writer.h"
#include "index_reader.h"

using namespace Acoustid;

TEST(IndexReaderTest, OpenEmpty)
{
	RAMDirectory dir;

	IndexReader reader(&dir);
	ASSERT_FALSE(dir.fileExists("info_0"));
	ASSERT_THROW(reader.open(), IOException);
}

TEST(IndexReaderTest, Search)
{
	RAMDirectory dir;

	IndexWriter writer(&dir);
	writer.open(true);
	uint32_t fp[] = { 7, 9, 12 };
	writer.addDocument(1, fp, 3);
	writer.commit();
	writer.addDocument(2, fp, 3);
	writer.commit();

	IndexReader reader(&dir);
	reader.open();
	
	TopHitsCollector collector(100);
	reader.search(fp, 3, &collector);
	ASSERT_EQ(2, collector.topResults().size());
	ASSERT_EQ(1, collector.topResults().at(0).id());
	ASSERT_EQ(3, collector.topResults().at(0).score());
	ASSERT_EQ(2, collector.topResults().at(1).id());
	ASSERT_EQ(3, collector.topResults().at(1).score());
}

