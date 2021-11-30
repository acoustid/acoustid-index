// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include "util/search_utils.h"

TEST(SearchUtilsTest, SearchFirstSmaller)
{
	int data[] = { 3, 5, 5, 6 };
	ASSERT_EQ(-1, searchFirstSmaller(data, 0, 4, 2));
	ASSERT_EQ(-1, searchFirstSmaller(data, 0, 4, 3));
	ASSERT_EQ(0, searchFirstSmaller(data, 0, 4, 4));
	ASSERT_EQ(0, searchFirstSmaller(data, 0, 4, 5));
	ASSERT_EQ(2, searchFirstSmaller(data, 0, 4, 6));
	ASSERT_EQ(3, searchFirstSmaller(data, 0, 4, 7));
	ASSERT_EQ(3, searchFirstSmaller(data, 0, 4, 100));
}

TEST(SearchUtilsTest, ScanLastGreater)
{
	int data[] = { 3, 5, 5, 6 };
	ASSERT_EQ(0, scanFirstGreater(data, 0, 4, 2));
	ASSERT_EQ(1, scanFirstGreater(data, 0, 4, 3));
	ASSERT_EQ(1, scanFirstGreater(data, 0, 4, 4));
	ASSERT_EQ(3, scanFirstGreater(data, 0, 4, 5));
	ASSERT_EQ(4, scanFirstGreater(data, 0, 4, 6));
	ASSERT_EQ(4, scanFirstGreater(data, 0, 4, 100));
}

