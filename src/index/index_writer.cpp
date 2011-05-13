// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <algorithm>
#include "store/directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "segment_index_writer.h"
#include "segment_data_writer.h"
#include "segment_index_reader.h"
#include "segment_data_reader.h"
#include "segment_merger.h"
#include "index_writer.h"

using namespace Acoustid;

IndexWriter::IndexWriter(Directory *dir)
	: IndexReader(dir), m_maxSegmentBufferSize(MAX_SEGMENT_BUFFER_SIZE)
{
	m_mergePolicy = new SegmentMergePolicy();
}

void IndexWriter::open(bool create)
{
	if (!m_infos.load(m_dir)) {
		if (create) {
			commit();
	 	}
		else {
			throw IOException("there is no index in the directory");
		}
	}
}

IndexWriter::~IndexWriter()
{
	delete m_mergePolicy;
}

void IndexWriter::addDocument(uint32_t id, uint32_t *terms, size_t length)
{
	for (size_t i = 0; i < length; i++) {
		m_segmentBuffer.push_back((uint64_t(terms[i]) << 32) | id);
	}
	maybeFlush();
}

void IndexWriter::commit()
{
	flush();
	m_infos.save(m_dir);
}

void IndexWriter::maybeFlush()
{
	if (m_segmentBuffer.size() > m_maxSegmentBufferSize) {
		flush();
	}
}

SegmentDataWriter *IndexWriter::segmentDataWriter(const SegmentInfo& info)
{
	OutputStream *indexOutput = m_dir->createFile(info.indexFileName());
	OutputStream *dataOutput = m_dir->createFile(info.dataFileName());
	SegmentIndexWriter *indexWriter = new SegmentIndexWriter(indexOutput);
	return new SegmentDataWriter(dataOutput, indexWriter, BLOCK_SIZE);
}

void IndexWriter::maybeMerge()
{
	const SegmentInfoList& segments = m_infos.segments();
	QList<int> merge = m_mergePolicy->findMerges(segments);
	if (merge.isEmpty()) {
		return;
	}
	//qDebug() << "Merging segments" << merge;

	SegmentInfo segment(m_infos.incLastSegmentId());
	{
		SegmentMerger merger(segmentDataWriter(segment));
		for (size_t i = 0; i < merge.size(); i++) {
			int j = merge.at(i);
			merger.addSource(new SegmentEnum(segmentIndex(j), segmentDataReader(j)));
		}
		merger.merge();
		segment.setBlockCount(merger.writer()->blockCount());
		segment.setLastKey(merger.writer()->lastKey());
	}

	IndexInfo info;
	info.setRevision(m_infos.revision());
	info.setLastSegmentId(m_infos.lastSegmentId());
	QSet<int> merged = merge.toSet();
	for (size_t i = 0; i < segments.size(); i++) {
		if (!merged.contains(i)) {
			info.addSegment(segments.at(i));
		}
		else {
			closeSegmentIndex(i);
		}
	}
	info.addSegment(segment);

	m_infos = info;
}

inline uint32_t itemKey(uint64_t item)
{
	return item >> 32;
}

inline uint32_t itemValue(uint64_t item)
{
	return item & 0xFFFFFFFF;
}

void IndexWriter::flush()
{
	if (m_segmentBuffer.empty()) {
		return;
	}
	//qDebug() << "Writing new segment" << (m_segmentBuffer.size() * 8.0 / 1024 / 1024);
	qSort(m_segmentBuffer.begin(), m_segmentBuffer.end());

	SegmentInfo info(m_infos.incLastSegmentId());
	ScopedPtr<SegmentDataWriter> writer(segmentDataWriter(info));
	uint64_t lastItem = UINT64_MAX;
	for (size_t i = 0; i < m_segmentBuffer.size(); i++) {
		uint64_t item = m_segmentBuffer[i];
		if (item != lastItem) {
			//qDebug() << "adding item" << key << value;
			writer->addItem(itemKey(item), itemValue(item));
			lastItem = item;
		}
	}
	writer->close();
	info.setBlockCount(writer->blockCount());
	info.setLastKey(writer->lastKey());

	m_infos.addSegment(info);
	maybeMerge();

	m_segmentBuffer.clear();
}

