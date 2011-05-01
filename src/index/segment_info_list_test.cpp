// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include "util/test_utils.h"
#include "store/ram_directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "segment_info_list.h"

using namespace Acoustid;

TEST(SegmentInfoListTest, FindCurrentRevision)
{
	RAMDirectory dir;

	int rev = SegmentInfoList::findCurrentRevision(&dir);
	ASSERT_EQ(-1, rev);

	delete dir.createFile("segments_0");
	rev = SegmentInfoList::findCurrentRevision(&dir);
	ASSERT_EQ(0, rev);

	delete dir.createFile("segments_1");
	rev = SegmentInfoList::findCurrentRevision(&dir);
	ASSERT_EQ(1, rev);

	delete dir.createFile("segments_8");
	rev = SegmentInfoList::findCurrentRevision(&dir);
	ASSERT_EQ(8, rev);
}

TEST(SegmentInfoListTest, Read)
{
	RAMDirectory dir;

	ScopedPtr<OutputStream> output(dir.createFile("segments_0"));
	output->writeVInt32(3);
	output->writeVInt32(2);
	output->writeVInt32(0);
	output->writeVInt32(42);
	output->writeVInt32(2);
	output->writeVInt32(66);
	output.reset();

	SegmentInfoList infos;
	ScopedPtr<InputStream> input(dir.openFile("segments_0"));
	infos.read(input.get());
	input.reset();

	ASSERT_EQ(3, infos.lastSegmentId());
	ASSERT_EQ(2, infos.segmentCount());
	ASSERT_EQ("segment_0", infos.info(0).name());
	ASSERT_EQ(42, infos.info(0).numDocs());
	ASSERT_EQ("segment_2", infos.info(1).name());
	ASSERT_EQ(66, infos.info(1).numDocs());
}

TEST(SegmentInfoListTest, Write)
{
	RAMDirectory dir;

	SegmentInfoList infos;
	infos.add(SegmentInfo(0, 42));
	infos.incLastSegmentId();
	infos.add(SegmentInfo(1, 66));
	infos.incLastSegmentId();
	ScopedPtr<OutputStream> output(dir.createFile("segments_0"));
	infos.write(output.get());
	output.reset();

	ScopedPtr<InputStream> input(dir.openFile("segments_0"));
	ASSERT_EQ(2, input->readVInt32());
	ASSERT_EQ(2, input->readVInt32());
	ASSERT_EQ(0, input->readVInt32());
	ASSERT_EQ(42, input->readVInt32());
	ASSERT_EQ(1, input->readVInt32());
	ASSERT_EQ(66, input->readVInt32());
}

TEST(SegmentInfoListTest, Clear)
{
	SegmentInfoList infos;
	infos.add(SegmentInfo(0, 42));
	infos.add(SegmentInfo(1, 66));
	ASSERT_EQ(2, infos.segmentCount());
	infos.clear();
	ASSERT_EQ(0, infos.segmentCount());
}

