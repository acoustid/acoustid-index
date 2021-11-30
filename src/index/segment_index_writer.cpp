// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "store/output_stream.h"
#include "segment_index_writer.h"

using namespace Acoustid;

SegmentIndexWriter::SegmentIndexWriter(OutputStream *output)
	: m_output(output)
{
}

SegmentIndexWriter::~SegmentIndexWriter()
{
	close();
}

void SegmentIndexWriter::addItem(uint32_t key)
{
	m_output->writeInt32(key);
}

void SegmentIndexWriter::close()
{
	m_output->flush();
}

