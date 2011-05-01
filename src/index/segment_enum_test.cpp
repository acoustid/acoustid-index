// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include <QFile>
#include "util/test_utils.h"
#include "store/ram_directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "segment_data_reader.h"
#include "segment_data_writer.h"
#include "segment_index.h"
#include "segment_index_reader.h"
#include "segment_index_writer.h"
#include "segment_enum.h"

using namespace Acoustid;

TEST(SegmentEnumTest, Iterate)
{
	RAMDirectory dir;

	{
		ScopedPtr<OutputStream> indexOutput(dir.createFile("segment_0.fii"));
		SegmentIndexWriter indexWriter(indexOutput.get());
		indexWriter.setBlockSize(8);

		ScopedPtr<OutputStream> dataOutput(dir.createFile("segment_0.fid"));
		SegmentDataWriter writer(dataOutput.get(), &indexWriter, indexWriter.blockSize());
		writer.addItem(200, 300);
		writer.addItem(201, 301);
		writer.addItem(201, 302);
		writer.addItem(202, 303);
		writer.close();
	}

	ScopedPtr<InputStream> indexInput(dir.openFile("segment_0.fii"));
	ScopedPtr<InputStream> dataInput(dir.openFile("segment_0.fid"));
	SegmentIndex *index = SegmentIndexReader(indexInput.get()).read();
	SegmentDataReader *dataReader = new SegmentDataReader(dataInput.get(), index->blockSize());

	SegmentEnum reader(index, dataReader);
	ASSERT_TRUE(reader.next());
	ASSERT_EQ(200, reader.key());
	ASSERT_EQ(300, reader.value());
	ASSERT_TRUE(reader.next());
	ASSERT_EQ(201, reader.key());
	ASSERT_EQ(301, reader.value());
	ASSERT_TRUE(reader.next());
	ASSERT_EQ(201, reader.key());
	ASSERT_EQ(302, reader.value());
	ASSERT_TRUE(reader.next());
	ASSERT_EQ(202, reader.key());
	ASSERT_EQ(303, reader.value());
	ASSERT_FALSE(reader.next());
}

