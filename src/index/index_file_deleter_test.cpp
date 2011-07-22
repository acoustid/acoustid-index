// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include "util/test_utils.h"
#include "store/ram_directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "index_file_deleter.h"

using namespace Acoustid;

TEST(IndexFileDeleterTest, DeleteOnDec)
{
	RAMDirectory dir;
	delete dir.createFile("test.txt");

	ASSERT_TRUE(dir.fileExists("test.txt"));
	IndexFileDeleter deleter(&dir);
	deleter.decRef("test.txt");
	ASSERT_FALSE(dir.fileExists("test.txt"));
}

TEST(IndexFileDeleterTest, DeleteOnIncDec)
{
	RAMDirectory dir;
	delete dir.createFile("test.txt");

	ASSERT_TRUE(dir.fileExists("test.txt"));
	IndexFileDeleter deleter(&dir);
	deleter.incRef("test.txt");
	deleter.decRef("test.txt");
	ASSERT_FALSE(dir.fileExists("test.txt"));
}

TEST(IndexFileDeleterTest, KeepOnInc)
{
	RAMDirectory dir;
	delete dir.createFile("test.txt");

	ASSERT_TRUE(dir.fileExists("test.txt"));
	IndexFileDeleter deleter(&dir);
	deleter.incRef("test.txt");
	ASSERT_TRUE(dir.fileExists("test.txt"));
}
