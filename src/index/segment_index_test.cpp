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
#include <QFile>
#include "util/test_utils.h"
#include "segment_index.h"

TEST(SegmentIndexTest, Sizes1)
{
	SegmentIndex index(256, 10, 1000);
	EXPECT_EQ(256, index.blockSize());
	EXPECT_EQ(10, index.indexInterval());
	EXPECT_EQ(3, index.levelCount());
	EXPECT_EQ(1000, index.levelKeyCount(0));
	EXPECT_EQ(100, index.levelKeyCount(1));
	EXPECT_EQ(10, index.levelKeyCount(2));
}

TEST(SegmentIndexTest, Sizes2)
{
	SegmentIndex index(256, 10, 1024);
	EXPECT_EQ(256, index.blockSize());
	EXPECT_EQ(10, index.indexInterval());
	EXPECT_EQ(3, index.levelCount());
	EXPECT_EQ(1024, index.levelKeyCount(0));
	EXPECT_EQ(102, index.levelKeyCount(1));
	EXPECT_EQ(10, index.levelKeyCount(2));
}

TEST(SegmentIndexTest, Sizes3)
{
	SegmentIndex index(256, 10, 1111);
	EXPECT_EQ(256, index.blockSize());
	EXPECT_EQ(10, index.indexInterval());
	EXPECT_EQ(4, index.levelCount());
	EXPECT_EQ(1111, index.levelKeyCount(0));
	EXPECT_EQ(111, index.levelKeyCount(1));
	EXPECT_EQ(11, index.levelKeyCount(2));
}

TEST(SegmentIndexTest, Rebuild)
{
	SegmentIndex index(256, 2, 8);
	ASSERT_EQ(3, index.levelCount());
	EXPECT_EQ(8, index.levelKeyCount(0));
	EXPECT_EQ(4, index.levelKeyCount(1));
	EXPECT_EQ(2, index.levelKeyCount(2));
	uint32_t *data = index.levelKeys(0);
	data[0] = 0;
	data[1] = 1;
	data[2] = 2;
	data[3] = 3;
	data[4] = 4;
	data[5] = 5;
	data[6] = 6;
	data[7] = 7;
	index.rebuild();
	uint32_t expected1[] = { 0, 2, 4, 6 };
	ASSERT_INTARRAY_EQ(expected1, index.levelKeys(1), 4);
	uint32_t expected2[] = { 0, 4 };
	ASSERT_INTARRAY_EQ(expected2, index.levelKeys(2), 2);
}
