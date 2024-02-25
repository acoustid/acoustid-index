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

	std::vector<uint32_t> fp1 = { 7, 9, 12 };
	std::vector<uint32_t> fp2 = { 7, 9, 11 };

	{
		auto writer = index->openWriter();
		writer->addDocument(1, fp1.data(), fp1.size());
		writer->commit();
		writer->addDocument(2, fp2.data(), fp2.size());
		writer->commit();
	}

	{
		IndexReader reader(index);
		auto results = reader.search(fp1);
		ASSERT_EQ(2, results.size());
		ASSERT_EQ(1, results.at(0).docId());
		ASSERT_EQ(3, results.at(0).score());
		ASSERT_EQ(2, results.at(1).docId());
		ASSERT_EQ(2, results.at(1).score());
	}
}

