// Acoustid Index -- Inverted index for audio fingerprints
// Copyright (C) 2011  Lukas Lalinsky
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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

