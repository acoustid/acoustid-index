// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "store/output_stream.h"
#include "segment_index_writer.h"

using namespace Acoustid;

SegmentIndexWriter::SegmentIndexWriter(OutputStream *output)
	: m_output(output), m_lastKey(0)
{
}

SegmentIndexWriter::~SegmentIndexWriter()
{
	close();
	delete m_output;
}

void SegmentIndexWriter::addItem(uint32_t key)
{
	m_output->writeVInt32(key - m_lastKey);
	m_lastKey = key;
}

void SegmentIndexWriter::close()
{
	m_output->flush();
}

