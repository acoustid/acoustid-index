// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include "util/test_utils.h"
#include "store/ram_directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "top_hits_collector.h"
#include "index.h"
#include "index_writer.h"
#include "index_reader.h"

using namespace Acoustid;

TEST(IndexReaderTest, Search)
{
	DirectorySharedPtr dir(new RAMDirectory());
	IndexSharedPtr index(new Index(dir, true));

	uint32_t fp1[] = { 7, 9, 12 };
    auto fp1len = 3;

	uint32_t fp2[] = { 7, 9, 11 };
    auto fp2len = 3;

	{
		auto writer = index->openWriter();
		writer->addDocument(1, fp1, fp1len);
		writer->commit();
		writer->addDocument(2, fp2, fp2len);
		writer->commit();
	}

	{
		IndexReader reader(index);
		TopHitsCollector collector(100);
		reader.search(fp1, fp1len, &collector);
		ASSERT_EQ(2, collector.topResults().size());
		ASSERT_EQ(1, collector.topResults().at(0).id());
		ASSERT_EQ(3, collector.topResults().at(0).score());
		ASSERT_EQ(2, collector.topResults().at(1).id());
		ASSERT_EQ(2, collector.topResults().at(1).score());
	}
}

