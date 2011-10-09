// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include "util/test_utils.h"
#include "store/ram_directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "index_info.h"

using namespace Acoustid;

TEST(IndexInfoTest, FindCurrentRevision)
{
	RAMDirectory dir;

	int rev = IndexInfo::findCurrentRevision(&dir);
	ASSERT_EQ(-1, rev);

	delete dir.createFile("info_0");
	rev = IndexInfo::findCurrentRevision(&dir);
	ASSERT_EQ(0, rev);

	delete dir.createFile("info_1");
	rev = IndexInfo::findCurrentRevision(&dir);
	ASSERT_EQ(1, rev);

	delete dir.createFile("info_8");
	rev = IndexInfo::findCurrentRevision(&dir);
	ASSERT_EQ(8, rev);
}

TEST(IndexInfoTest, ReadFromDir)
{
	RAMDirectory dir;

	ScopedPtr<OutputStream> output(dir.createFile("info_0"));
	output->writeVInt32(2);
	output->writeVInt32(2);
	output->writeVInt32(0);
	output->writeVInt32(42);
	output->writeVInt32(100);
	output->writeVInt32(123);
	output->writeVInt32(1);
	output->writeVInt32(66);
	output->writeVInt32(200);
	output->writeVInt32(456);
	output->writeInt32(3627580765u);
	output.reset();

	IndexInfo infos;
	infos.load(&dir);

	ASSERT_EQ(2, infos.lastSegmentId());
	ASSERT_EQ(2, infos.segmentCount());
	ASSERT_EQ("segment_0", infos.segment(0).name());
	ASSERT_EQ(42, infos.segment(0).blockCount());
	ASSERT_EQ(100, infos.segment(0).lastKey());
	ASSERT_EQ(123, infos.segment(0).checksum());
	ASSERT_EQ("segment_1", infos.segment(1).name());
	ASSERT_EQ(66, infos.segment(1).blockCount());
	ASSERT_EQ(200, infos.segment(1).lastKey());
	ASSERT_EQ(456, infos.segment(1).checksum());
}

TEST(IndexInfoTest, ReadFromDirCorruptRecover)
{
	RAMDirectory dir;

	{
		ScopedPtr<OutputStream> output(dir.createFile("info_0"));
		output->writeVInt32(2);
		output->writeVInt32(2);
		output->writeVInt32(0);
		output->writeVInt32(42);
		output->writeVInt32(100);
		output->writeVInt32(123);
		output->writeVInt32(1);
		output->writeVInt32(66);
		output->writeVInt32(200);
		output->writeVInt32(456);
		output->writeInt32(3627580765u);
	}

	{
		ScopedPtr<OutputStream> output(dir.createFile("info_1"));
		output->writeVInt32(3);
	}

	IndexInfo infos;
	infos.load(&dir);

	ASSERT_EQ(0, infos.revision());
	ASSERT_EQ(2, infos.lastSegmentId());
	ASSERT_EQ(2, infos.segmentCount());
	ASSERT_EQ("segment_0", infos.segment(0).name());
	ASSERT_EQ(42, infos.segment(0).blockCount());
	ASSERT_EQ(100, infos.segment(0).lastKey());
	ASSERT_EQ(123, infos.segment(0).checksum());
	ASSERT_EQ("segment_1", infos.segment(1).name());
	ASSERT_EQ(66, infos.segment(1).blockCount());
	ASSERT_EQ(200, infos.segment(1).lastKey());
	ASSERT_EQ(456, infos.segment(1).checksum());
}

TEST(IndexInfoTest, ReadFromDirCorruptRecover2)
{
	RAMDirectory dir;

	{
		ScopedPtr<OutputStream> output(dir.createFile("info_0"));
		output->writeVInt32(2);
		output->writeVInt32(2);
		output->writeVInt32(0);
		output->writeVInt32(42);
		output->writeVInt32(100);
		output->writeVInt32(123);
		output->writeVInt32(1);
		output->writeVInt32(66);
		output->writeVInt32(200);
		output->writeVInt32(456);
		output->writeInt32(3627580765u);
	}

	{
		ScopedPtr<OutputStream> output(dir.createFile("info_1"));
		output->writeVInt32(3);
	}

	{
		ScopedPtr<OutputStream> output(dir.createFile("info_2"));
		output->writeVInt32(4);
	}

	IndexInfo infos;
	infos.load(&dir);

	ASSERT_EQ(0, infos.revision());
	ASSERT_EQ(2, infos.lastSegmentId());
	ASSERT_EQ(2, infos.segmentCount());
	ASSERT_EQ("segment_0", infos.segment(0).name());
	ASSERT_EQ(42, infos.segment(0).blockCount());
	ASSERT_EQ(100, infos.segment(0).lastKey());
	ASSERT_EQ(123, infos.segment(0).checksum());
	ASSERT_EQ("segment_1", infos.segment(1).name());
	ASSERT_EQ(66, infos.segment(1).blockCount());
	ASSERT_EQ(200, infos.segment(1).lastKey());
	ASSERT_EQ(456, infos.segment(1).checksum());
}

TEST(IndexInfoTest, ReadFromDirCorruptFail)
{
	RAMDirectory dir;

	{
		ScopedPtr<OutputStream> output(dir.createFile("info_0"));
		output->writeVInt32(2);
	}

	{
		ScopedPtr<OutputStream> output(dir.createFile("info_1"));
		output->writeVInt32(3);
	}

	IndexInfo infos;
	ASSERT_THROW(infos.load(&dir), CorruptIndexException);
}

TEST(IndexInfoTest, WriteIntoDir)
{
	RAMDirectory dir;

	IndexInfo infos;
	infos.addSegment(SegmentInfo(0, 42, 100, 123));
	infos.incLastSegmentId();
	infos.addSegment(SegmentInfo(1, 66, 200, 456));
	infos.incLastSegmentId();
	infos.save(&dir);

	ScopedPtr<InputStream> input(dir.openFile("info_0"));
	ASSERT_EQ(2, input->readVInt32());
	ASSERT_EQ(2, input->readVInt32());
	ASSERT_EQ(0, input->readVInt32());
	ASSERT_EQ(42, input->readVInt32());
	ASSERT_EQ(100, input->readVInt32());
	ASSERT_EQ(123, input->readVInt32());
	ASSERT_EQ(1, input->readVInt32());
	ASSERT_EQ(66, input->readVInt32());
	ASSERT_EQ(200, input->readVInt32());
	ASSERT_EQ(456, input->readVInt32());
	ASSERT_EQ(3627580765u, input->readInt32());
}

TEST(IndexInfoTest, Clear)
{
	IndexInfo infos;
	infos.addSegment(SegmentInfo(0, 42));
	infos.addSegment(SegmentInfo(1, 66));
	ASSERT_EQ(2, infos.segmentCount());
	infos.clearSegments();
	ASSERT_EQ(0, infos.segmentCount());
}

