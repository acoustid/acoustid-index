#include "segment_data_writer.h"

SegmentDataWriter::SegmentDataWriter(OutputStream *output)
	: m_output(output)
{
}

SegmentDataWriter::~SegmentDataWriter()
{
}

void SegmentDataWriter::addItem(uint32_t key, uint32_t value)
{
}

