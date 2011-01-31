#ifndef ACOUSTID_INDEX_SEGMENT_DATA_WRITER_H_
#define ACOUSTID_INDEX_SEGMENT_DATA_WRITER_H_

#include "common.h"

class OutputStream;

class SegmentDataWriter
{
public:
	SegmentDataWriter(OutputStream *output);
	virtual ~SegmentDataWriter();

	virtual void addItem(uint32_t key, uint32_t value);

private:
	OutputStream *m_output;
};

#endif
