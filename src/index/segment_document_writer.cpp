// Copyright (C) 2013  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "store/output_stream.h"
#include "segment_document_writer.h"

using namespace Acoustid;

SegmentDocumentWriter::SegmentDocumentWriter(OutputStream *indexOutput, OutputStream *dataOutput)
	: m_indexOutput(indexOutput), m_dataOutput(dataOutput), m_checksum(0)
{
}

SegmentDocumentWriter::~SegmentDocumentWriter()
{
	close();
}

void SegmentDocumentWriter::addDocument(uint32_t id, uint32_t *data, size_t length)
{
	assert(m_positions.find(id) == m_positions.end());
	m_positions[id] = m_dataOutput->position();
	m_dataOutput->writeVInt32(length);
	for (size_t i = 0; i < length; i++) {
		m_checksum ^= data[i];
		m_dataOutput->writeInt32(data[i]);
	}
}

void SegmentDocumentWriter::close()
{
	m_index = SegmentDocumentIndexSharedPtr(new SegmentDocumentIndex(m_positions.size()));

	uint32_t *ids = m_index->ids();
	size_t *positions = m_index->positions();

	for (std::map<uint32_t, size_t>::iterator it = m_positions.begin(); it != m_positions.end(); ++it) {
		*ids++ = it->first;
		*positions++ = it->second;
		m_indexOutput->writeVInt32(it->first);
		m_indexOutput->writeVInt64(it->second);
	}

	m_indexOutput->flush();
	m_dataOutput->flush();
}

