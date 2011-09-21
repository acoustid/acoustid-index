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
#include "index.h"
#include "index_file_deleter.h"
#include "index_writer.h"

using namespace Acoustid;

IndexWriter::IndexWriter(Directory *dir, const IndexInfo& info, const SegmentIndexMap& indexes, Index* index)
	: IndexReader(dir, info, indexes, index), m_maxSegmentBufferSize(MAX_SEGMENT_BUFFER_SIZE)
{
	m_mergePolicy.reset(new SegmentMergePolicy());
}

IndexWriter::~IndexWriter()
{
	qDebug() << "IndexWriter closed" << this << m_index;
	if (m_index) {
		for (int i = 0; i < m_newSegments.size(); i++) {
			m_index->fileDeleter()->decRef(m_newSegments.at(i));
		}
		m_index->onWriterDeleted(this);
	}
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
	m_info.save(m_dir);
	qDebug() << "Committed revision" << m_info.revision();
	if (m_index) {
		m_index->refresh(m_info);
		for (int i = 0; i < m_newSegments.size(); i++) {
			m_index->fileDeleter()->decRef(m_newSegments.at(i));
		}
	}
	m_newSegments.clear();
}

void IndexWriter::maybeFlush()
{
	if (m_segmentBuffer.size() > m_maxSegmentBufferSize) {
		flush();
	}
}

SegmentDataWriter* IndexWriter::segmentDataWriter(const SegmentInfo& segment)
{
	OutputStream* indexOutput = m_dir->createFile(segment.indexFileName());
	OutputStream* dataOutput = m_dir->createFile(segment.dataFileName());
	SegmentIndexWriter* indexWriter = new SegmentIndexWriter(indexOutput);
	return new SegmentDataWriter(dataOutput, indexWriter, BLOCK_SIZE);
}

void IndexWriter::merge(const QList<int>& merge)
{
	if (merge.isEmpty()) {
		return;
	}

	uint32_t expectedChecksum = 0;
	const SegmentInfoList& segments = m_info.segments();
	SegmentInfo segment(m_info.incLastSegmentId());
	{
		SegmentMerger merger(segmentDataWriter(segment));
		for (size_t i = 0; i < merge.size(); i++) {
			int j = merge.at(i);
			const SegmentInfo& s = segments.at(j);
			expectedChecksum ^= s.checksum();
			qDebug() << "Merging segment" << s.id() << "into" << segment.id();
			merger.addSource(new SegmentEnum(segmentIndex(s), segmentDataReader(s)));
		}
		merger.merge();
		segment.setBlockCount(merger.writer()->blockCount());
		segment.setLastKey(merger.writer()->lastKey());
		segment.setChecksum(merger.writer()->checksum());
	}

	if (segment.checksum() != expectedChecksum) {
		qDebug() << "Checksum mismatch after merge, got" << segment.checksum() << ", expected" << expectedChecksum;
		qFatal("Checksum mismatch after merge");
	}

	qDebug() << "New segment" << segment.id();
	m_newSegments.append(segment);
	if (m_index) {
		m_index->fileDeleter()->incRef(segment);
	}

	SegmentInfoList newSegments;
	QSet<int> merged = merge.toSet();
	for (size_t i = 0; i < segments.size(); i++) {
		const SegmentInfo& s = segments.at(i);
		if (!merged.contains(i)) {
			newSegments.append(s);
		}
	}
	newSegments.append(segment);
	m_info.setSegments(newSegments);
}

void IndexWriter::maybeMerge()
{
	const SegmentInfoList& segments = m_info.segments();
	merge(m_mergePolicy->findMerges(segments));
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
	std::sort(m_segmentBuffer.begin(), m_segmentBuffer.end());

	SegmentInfo segment(m_info.incLastSegmentId());
	ScopedPtr<SegmentDataWriter> writer(segmentDataWriter(segment));
	uint64_t lastItem = UINT64_MAX;
	for (size_t i = 0; i < m_segmentBuffer.size(); i++) {
		uint64_t item = m_segmentBuffer[i];
		if (item != lastItem) {
			uint32_t value = itemValue(item);
			writer->addItem(itemKey(item), value);
			lastItem = item;
		}
	}
	writer->close();
	segment.setBlockCount(writer->blockCount());
	segment.setLastKey(writer->lastKey());
	segment.setChecksum(writer->checksum());

	qDebug() << "New segment" << segment.id();
	m_newSegments.append(segment);
	if (m_index) {
		m_index->fileDeleter()->incRef(segment);
	}

	m_info.addSegment(segment);
	m_indexes = Index::loadSegmentIndexes(m_dir, m_info, m_indexes);
	maybeMerge();

	m_segmentBuffer.clear();
}

void IndexWriter::optimize()
{
	flush();

	const SegmentInfoList& segments = m_info.segments();
	QList<int> merges;
	for (int i = 0; i < segments.size(); i++) {
		merges.append(i);
	}
	merge(merges);
}

void IndexWriter::cleanup()
{
	flush();

	QSet<QString> usedFileNames;
	usedFileNames.insert(m_info.indexInfoFileName(m_info.revision()));
	const SegmentInfoList& segments = m_info.segments();
	for (int i = 0; i < segments.size(); i++) {
		const SegmentInfo& segment = segments.at(i);
		usedFileNames.insert(segment.indexFileName());
		usedFileNames.insert(segment.dataFileName());
	}

	QList<QString> allFileNames = m_dir->listFiles();
	for (int i = 0; i < allFileNames.size(); i++) {
		if (!usedFileNames.contains(allFileNames.at(i))) {
			m_dir->deleteFile(allFileNames.at(i));
		}
	}
}

