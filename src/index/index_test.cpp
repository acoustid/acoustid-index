// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include "util/test_utils.h"
#include "store/ram_directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "index.h"
#include "index_writer.h"

using namespace Acoustid;

TEST(IndexTest, OpenEmpty)
{
	DirectorySharedPtr dir(new RAMDirectory());
	ASSERT_FALSE(dir->fileExists("info_0"));
	ASSERT_THROW({ Index index(dir); }, IOException);
}

TEST(IndexTest, OpenEmptyCreate)
{
	DirectorySharedPtr dir(new RAMDirectory());
	ASSERT_FALSE(dir->fileExists("info_0"));
	Index index(dir, true);
	ASSERT_TRUE(dir->fileExists("info_0"));
}

TEST(IndexTest, DeleteUnusedFiled)
{
	DirectorySharedPtr dir(new RAMDirectory());
	IndexSharedPtr index(new Index(dir, true));

	ASSERT_TRUE(index->directory()->fileExists("info_0"));
	{
		std::unique_ptr<IndexWriter> writer(new IndexWriter(index));
		uint32_t fp[] = { 1, 2, 3 };
		writer->addDocument(1, fp, 3);
		writer->commit();
	}
	ASSERT_TRUE(index->directory()->fileExists("info_1"));
	ASSERT_FALSE(index->directory()->fileExists("info_0"));
}
