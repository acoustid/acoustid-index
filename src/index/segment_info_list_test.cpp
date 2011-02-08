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
	output->writeVInt32(2);
	output->writeString("segment_0");
	output->writeVInt32(42);
	output->writeString("segment_1");
	output->writeVInt32(66);
	output.reset();

	SegmentInfoList infos;
	ScopedPtr<InputStream> input(dir.openFile("segments_0"));
	infos.read(input.get());
	input.reset();

	ASSERT_EQ(2, infos.segmentCount());
	ASSERT_EQ("segment_0", infos.info(0).name());
	ASSERT_EQ(42, infos.info(0).numDocs());
	ASSERT_EQ("segment_1", infos.info(1).name());
	ASSERT_EQ(66, infos.info(1).numDocs());
}

TEST(SegmentInfoListTest, Write)
{
	RAMDirectory dir;

	SegmentInfoList infos;
	infos.add(SegmentInfo("segment_0", 42));
	infos.add(SegmentInfo("segment_1", 66));
	ScopedPtr<OutputStream> output(dir.createFile("segments_0"));
	infos.write(output.get());
	output.reset();

	ScopedPtr<InputStream> input(dir.openFile("segments_0"));
	ASSERT_EQ(2, input->readVInt32());
	ASSERT_EQ("segment_0", input->readString());
	ASSERT_EQ(42, input->readVInt32());
	ASSERT_EQ("segment_1", input->readString());
	ASSERT_EQ(66, input->readVInt32());
}

TEST(SegmentInfoListTest, Clear)
{
	SegmentInfoList infos;
	infos.add(SegmentInfo("segment_0", 42));
	infos.add(SegmentInfo("segment_1", 66));
	ASSERT_EQ(2, infos.segmentCount());
	infos.clear();
	ASSERT_EQ(0, infos.segmentCount());
}

