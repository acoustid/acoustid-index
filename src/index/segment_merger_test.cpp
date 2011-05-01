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
#include "segment_merger.h"

using namespace Acoustid;

TEST(SegmentMergerTest, Iterate)
{
	RAMDirectory dir;
	size_t numDocs;

	{
		OutputStream *indexOutput = dir.createFile("segment_0.fii");
		SegmentIndexWriter *indexWriter = new SegmentIndexWriter(indexOutput);
		indexWriter->setBlockSize(8);

		OutputStream *dataOutput = dir.createFile("segment_0.fid");
		SegmentDataWriter writer(dataOutput, indexWriter, indexWriter->blockSize());
		writer.addItem(200, 300);
		writer.addItem(201, 301);
		writer.addItem(201, 302);
		writer.addItem(202, 303);
		writer.close();
	}

	{
		OutputStream *indexOutput = dir.createFile("segment_1.fii");
		SegmentIndexWriter *indexWriter = new SegmentIndexWriter(indexOutput);
		indexWriter->setBlockSize(8);

		OutputStream *dataOutput = dir.createFile("segment_1.fid");
		SegmentDataWriter writer(dataOutput, indexWriter, indexWriter->blockSize());
		writer.addItem(199, 500);
		writer.addItem(201, 300);
		writer.addItem(201, 304);
		writer.addItem(202, 303);
		writer.addItem(500, 501);
		writer.close();
	}

	{
		OutputStream *indexOutput = dir.createFile("segment_2.fii");
		SegmentIndexWriter *indexWriter = new SegmentIndexWriter(indexOutput);
		indexWriter->setBlockSize(8);

		OutputStream *dataOutput(dir.createFile("segment_2.fid"));
		SegmentDataWriter *writer = new SegmentDataWriter(dataOutput, indexWriter, indexWriter->blockSize());

		InputStream *indexInput1 = dir.openFile("segment_0.fii");
		InputStream *dataInput1 = dir.openFile("segment_0.fid");
		SegmentIndexSharedPtr index1 = SegmentIndexReader(indexInput1).read();
		SegmentDataReader *dataReader1 = new SegmentDataReader(dataInput1, index1->blockSize());
		SegmentEnum *reader1 = new SegmentEnum(index1, dataReader1);

		InputStream *indexInput2 = dir.openFile("segment_1.fii");
		InputStream *dataInput2 = dir.openFile("segment_1.fid");
		SegmentIndexSharedPtr index2 = SegmentIndexReader(indexInput2).read();
		SegmentDataReader *dataReader2 = new SegmentDataReader(dataInput2, index2->blockSize());
		SegmentEnum *reader2 = new SegmentEnum(index2, dataReader2);

		SegmentMerger merger(writer);
		merger.addSource(reader1);
		merger.addSource(reader2);
		numDocs = merger.merge();
	}

	InputStream *indexInput = dir.openFile("segment_2.fii");
	InputStream *dataInput = dir.openFile("segment_2.fid");
	SegmentIndexSharedPtr index = SegmentIndexReader(indexInput).read();
	SegmentDataReader *dataReader = new SegmentDataReader(dataInput, index->blockSize());
	SegmentEnum reader(index, dataReader);

	ASSERT_EQ(7, numDocs);
	ASSERT_TRUE(reader.next());
	ASSERT_EQ(199, reader.key());
	ASSERT_EQ(500, reader.value());
	ASSERT_TRUE(reader.next());
	ASSERT_EQ(200, reader.key());
	ASSERT_EQ(300, reader.value());
	ASSERT_TRUE(reader.next());
	ASSERT_EQ(201, reader.key());
	ASSERT_EQ(300, reader.value());
	ASSERT_TRUE(reader.next());
	ASSERT_EQ(201, reader.key());
	ASSERT_EQ(301, reader.value());
	ASSERT_TRUE(reader.next());
	ASSERT_EQ(201, reader.key());
	ASSERT_EQ(302, reader.value());
	ASSERT_TRUE(reader.next());
	ASSERT_EQ(201, reader.key());
	ASSERT_EQ(304, reader.value());
	ASSERT_TRUE(reader.next());
	ASSERT_EQ(202, reader.key());
	ASSERT_EQ(303, reader.value());
	ASSERT_TRUE(reader.next());
	ASSERT_EQ(500, reader.key());
	ASSERT_EQ(501, reader.value());
	ASSERT_FALSE(reader.next());
}

