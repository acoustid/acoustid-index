// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include <QFile>
#include "util/test_utils.h"
#include "segment_merge_policy.h"

using namespace Acoustid;

TEST(SegmentMergePolicyTest, TestFindMerges)
{
	SegmentMergePolicy policy(2, 2);

	SegmentInfoList infos;
	infos.append(SegmentInfo(0, 1));
	infos.append(SegmentInfo(1, 1));
	infos.append(SegmentInfo(2, 1));

	int expected[] = { 0, 1 };
	QList<int> merge = policy.findMerges(infos);
	ASSERT_EQ(2, merge.size());
	ASSERT_INTARRAY_EQ(expected, merge, 2);
}

TEST(SegmentMergePolicyTest, TestFindMerges2)
{
	SegmentMergePolicy policy(2, 2);

	SegmentInfoList infos;
	infos.append(SegmentInfo(0, 3));
	infos.append(SegmentInfo(1, 2));
	infos.append(SegmentInfo(2, 1));
	infos.append(SegmentInfo(3, 1));
	infos.append(SegmentInfo(4, 1));

	int expected[] = { 2, 3 };
	QList<int> merge = policy.findMerges(infos);
	ASSERT_EQ(2, merge.size());
	ASSERT_INTARRAY_EQ(expected, merge, 2);
}

TEST(SegmentMergePolicyTest, TestFindMerges3)
{
	SegmentMergePolicy policy(2, 2);

	SegmentInfoList infos;
	infos.append(SegmentInfo(0, 3));
	infos.append(SegmentInfo(1, 2));
	infos.append(SegmentInfo(4, 1));
	infos.append(SegmentInfo(5, 2));

	QList<int> merge = policy.findMerges(infos);
	ASSERT_EQ(0, merge.size());
}

TEST(SegmentMergePolicyTest, TestFindMerges4)
{
	SegmentMergePolicy policy(2, 2);

	SegmentInfoList infos;
	infos.append(SegmentInfo(0, 3));
	infos.append(SegmentInfo(1, 2));
	infos.append(SegmentInfo(4, 1));
	infos.append(SegmentInfo(5, 2));
	infos.append(SegmentInfo(6, 1));

	int expected[] = { 2, 4 };
	QList<int> merge = policy.findMerges(infos);
	ASSERT_EQ(2, merge.size());
	ASSERT_INTARRAY_EQ(expected, merge, 2);
}

TEST(SegmentMergePolicyTest, TestFindMergesTooLarge)
{
	SegmentMergePolicy policy(2, 2);

	SegmentInfoList infos;
	infos.append(SegmentInfo(0, 3));
	infos.append(SegmentInfo(1, 2));
	infos.append(SegmentInfo(4, 1));
	infos.append(SegmentInfo(5, 2));
	infos.append(SegmentInfo(6, 1));
	infos.append(SegmentInfo(7, 2 * 1024 * 1024));

	int expected[] = { 2, 4 };
	QList<int> merge = policy.findMerges(infos);
	ASSERT_EQ(2, merge.size());
	ASSERT_INTARRAY_EQ(expected, merge, 2);
}

