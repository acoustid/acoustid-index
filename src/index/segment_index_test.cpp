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

using namespace Acoustid;

TEST(SegmentIndexTest, Sizes1)
{
	SegmentIndex index(256, 1000);
	EXPECT_EQ(256, index.blockSize());
	EXPECT_EQ(1, index.levelCount());
	EXPECT_EQ(1000, index.levelKeyCount(0));
}

TEST(SegmentIndexTest, Sizes2)
{
	SegmentIndex index(256, 1024);
	EXPECT_EQ(256, index.blockSize());
	EXPECT_EQ(1, index.levelCount());
	EXPECT_EQ(1024, index.levelKeyCount(0));
}

TEST(SegmentIndexTest, Sizes3)
{
	SegmentIndex index(256, 1111);
	EXPECT_EQ(256, index.blockSize());
	EXPECT_EQ(1, index.levelCount());
	EXPECT_EQ(1111, index.levelKeyCount(0));
}

TEST(SegmentIndexTest, Search)
{
	size_t firstBlock = 0, lastBlock = 0;
	SegmentIndex index(256, 8);
	ASSERT_EQ(1, index.levelCount());
	EXPECT_EQ(8, index.levelKeyCount(0));
	uint32_t *data = index.levelKeys(0);
	data[0] = 1;
	data[1] = 2;
	data[2] = 2;
	data[3] = 2;
	data[4] = 4;
	data[5] = 5;
	data[6] = 8;
	data[7] = 9;
	index.rebuild();

	EXPECT_FALSE(index.search(0, &firstBlock, &lastBlock));

	EXPECT_TRUE(index.search(1, &firstBlock, &lastBlock));
	EXPECT_EQ(0, firstBlock);
	EXPECT_EQ(0, lastBlock);

	EXPECT_TRUE(index.search(2, &firstBlock, &lastBlock));
	EXPECT_EQ(0, firstBlock);
	EXPECT_EQ(3, lastBlock);

	EXPECT_TRUE(index.search(3, &firstBlock, &lastBlock));
	EXPECT_EQ(3, firstBlock);
	EXPECT_EQ(3, lastBlock);

	EXPECT_TRUE(index.search(4, &firstBlock, &lastBlock));
	EXPECT_EQ(3, firstBlock);
	EXPECT_EQ(4, lastBlock);

	EXPECT_TRUE(index.search(5, &firstBlock, &lastBlock));
	EXPECT_EQ(4, firstBlock);
	EXPECT_EQ(5, lastBlock);

	EXPECT_TRUE(index.search(6, &firstBlock, &lastBlock));
	EXPECT_EQ(5, firstBlock);
	EXPECT_EQ(5, lastBlock);

	EXPECT_TRUE(index.search(7, &firstBlock, &lastBlock));
	EXPECT_EQ(5, firstBlock);
	EXPECT_EQ(5, lastBlock);

	EXPECT_TRUE(index.search(8, &firstBlock, &lastBlock));
	EXPECT_EQ(5, firstBlock);
	EXPECT_EQ(6, lastBlock);

	EXPECT_TRUE(index.search(9, &firstBlock, &lastBlock));
	EXPECT_EQ(6, firstBlock);
	EXPECT_EQ(7, lastBlock);

	EXPECT_TRUE(index.search(10, &firstBlock, &lastBlock));
	EXPECT_EQ(7, firstBlock);
	EXPECT_EQ(7, lastBlock);

	EXPECT_TRUE(index.search(100, &firstBlock, &lastBlock));
	EXPECT_EQ(7, firstBlock);
	EXPECT_EQ(7, lastBlock);
}

