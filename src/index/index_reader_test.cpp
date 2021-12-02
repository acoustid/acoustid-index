// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include "util/test_utils.h"
#include "store/ram_directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "index.h"
#include "index_writer.h"
#include "index_reader.h"

using namespace Acoustid;

TEST(IndexReaderTest, Search)
{
	DirectorySharedPtr dir(new RAMDirectory());
	IndexSharedPtr index(new Index(dir, true));

	{
		IndexWriter writer(index);
		writer.insertOrUpdateDocument(1, { 7, 9, 12});
		writer.commit();
		writer.insertOrUpdateDocument(2, { 7, 9, 11 });
		writer.commit();
	}

	{
		IndexReader reader(index);
		auto results = reader.search({ 7, 9, 12 });
		ASSERT_EQ(2, results.size());
		ASSERT_EQ(3, results.at(0).score());
		ASSERT_EQ(1, results.at(0).docId());
		ASSERT_EQ(2, results.at(1).score());
		ASSERT_EQ(2, results.at(1).docId());
	}
}

