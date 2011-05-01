// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "store/output_stream.h"
#include "segment_index_writer.h"

using namespace Acoustid;

SegmentIndexWriter::SegmentIndexWriter(OutputStream *output)
	: m_output(output), m_blockSize(0),
	  m_headerWritten(false), m_keyCount(0), m_lastKey(0),
	  m_keyCountPosition(0)
{
}

SegmentIndexWriter::~SegmentIndexWriter()
{
	close();
	delete m_output;
}

void SegmentIndexWriter::maybeWriteHeader()
{
	if (!m_headerWritten) {
		m_output->writeInt32(m_blockSize);
		m_keyCountPosition = m_output->position();
		m_output->writeInt32(0);
		m_headerWritten = true;
	}
}

void SegmentIndexWriter::addItem(uint32_t key)
{
	maybeWriteHeader();
	m_output->writeVInt32(key - m_lastKey);
	m_lastKey = key;
	m_keyCount++;
}

void SegmentIndexWriter::close()
{
	maybeWriteHeader();
	m_output->seek(m_keyCountPosition);
	m_output->writeInt32(m_keyCount);
	m_output->flush();
}

