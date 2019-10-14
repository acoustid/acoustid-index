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
#include "index_utils.h"
#include "index_writer.h"

using namespace Acoustid;

IndexWriter::IndexWriter(DirectorySharedPtr dir, const IndexInfo& info)
	: IndexReader(dir, info), m_maxSegmentBufferSize(MAX_SEGMENT_BUFFER_SIZE), m_maxDocumentId(0)
{
	m_mergePolicy.reset(new SegmentMergePolicy());
}

IndexWriter::IndexWriter(IndexSharedPtr index)
	: IndexReader(index), m_maxSegmentBufferSize(MAX_SEGMENT_BUFFER_SIZE), m_maxDocumentId(0)
{
	m_index->acquireWriterLock();
	m_mergePolicy.reset(new SegmentMergePolicy());
}

IndexWriter::~IndexWriter()
{
	if (m_index) {
		m_index->releaseWriterLock();
	}
}

void IndexWriter::addDocument(uint32_t id, uint32_t *terms, size_t length)
{
	for (size_t i = 0; i < length; i++) {
		m_segmentBuffer.push_back(packItem(terms[i], id));
	}
	if (id > m_maxDocumentId) {
		m_maxDocumentId = id;
	}
	maybeFlush();
}

void IndexWriter::setAttribute(const QString& name, const QString& value)
{
	m_info.setAttribute(name, value);
}

void IndexWriter::commit()
{
	flush();
	IndexInfo info(m_info);
	info.save(m_dir.data());
	if (m_index) {
		m_index->updateInfo(m_info, info, true);
	}
	m_info = info;
	qDebug() << "Committed revision" << m_info.revision() << m_info.segments().size();
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
	IndexInfo info(m_info);
	SegmentInfo segment(info.incLastSegmentId());
	{
		SegmentMerger merger(segmentDataWriter(segment));
		for (size_t i = 0; i < merge.size(); i++) {
			int j = merge.at(i);
			const SegmentInfo& s = segments.at(j);
			expectedChecksum ^= s.checksum();
			qDebug() << "Merging segment" << s.id() << "with checksum" << s.checksum() << "into segment" << segment.id();
			merger.addSource(new SegmentEnum(s.index(), segmentDataReader(s)));
		}
		merger.merge();
		segment.setBlockCount(merger.writer()->blockCount());
		segment.setLastKey(merger.writer()->lastKey());
		segment.setChecksum(merger.writer()->checksum());
		segment.setIndex(merger.writer()->index());
	}

	qDebug() << "New segment" << segment.id() << "with checksum" << segment.checksum() << "(merge)";

	if (segment.checksum() != expectedChecksum) {
		throw CorruptIndexException("checksum mismatch after merge");
	}

	QSet<int> merged = merge.toSet();
	info.clearSegments();
	for (size_t i = 0; i < segments.size(); i++) {
		const SegmentInfo& s = segments.at(i);
		if (!merged.contains(i)) {
			info.addSegment(s);
		}
	}
	info.addSegment(segment);
	if (m_index) {
		m_index->updateInfo(m_info, info);
	}
	m_info = info;
}

void IndexWriter::maybeMerge()
{
	const SegmentInfoList& segments = m_info.segments();
	merge(m_mergePolicy->findMerges(segments));
}

void IndexWriter::flush()
{
	if (m_segmentBuffer.empty()) {
		return;
	}
	//qDebug() << "Writing new segment" << (m_segmentBuffer.size() * 8.0 / 1024 / 1024);
	std::sort(m_segmentBuffer.begin(), m_segmentBuffer.end());

	IndexInfo info(m_info);
	SegmentInfo segment(info.incLastSegmentId());
	{
		ScopedPtr<SegmentDataWriter> writer(segmentDataWriter(segment));
		uint64_t lastItem = UINT64_MAX;
		for (size_t i = 0; i < m_segmentBuffer.size(); i++) {
			uint64_t item = m_segmentBuffer[i];
			if (item != lastItem) {
				uint32_t value = unpackItemValue(item);
				writer->addItem(unpackItemKey(item), value);
				lastItem = item;
			}
		}
		writer->close();
		segment.setBlockCount(writer->blockCount());
		segment.setLastKey(writer->lastKey());
		segment.setChecksum(writer->checksum());
		segment.setIndex(writer->index());
	}

	qDebug() << "New segment" << segment.id() << "with checksum" << segment.checksum();
	info.addSegment(segment);
	if (info.attribute("max_document_id").toInt() < m_maxDocumentId) {
		info.setAttribute("max_document_id", QString::number(m_maxDocumentId));
	}
	if (m_index) {
		m_index->updateInfo(m_info, info);
	}
	m_info = info;

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

