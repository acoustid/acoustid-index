#ifndef ACOUSTID_INDEX_SEGMENT_INDEX_WRITER_H_
#define ACOUSTID_INDEX_SEGMENT_INDEX_WRITER_H_

#include "common.h"

class OutputStream;

class SegmentIndexWriter
{
public:
	SegmentIndexWriter(OutputStream *output);
	virtual ~SegmentIndexWriter();

	virtual void addItem(uint32_t key);

private:
	OutputStream *m_output;
};

#endif
