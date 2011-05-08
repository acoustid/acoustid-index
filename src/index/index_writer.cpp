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
	m_revision = SegmentInfoList::findCurrentRevision(m_dir);
	if (m_revision != -1) {
		m_infos.read(m_dir->openFile(SegmentInfoList::segmentsFileName(m_revision)));
	}
	else if (create) {
		commit();
	}
	else {
		throw IOException("there is no index in the directory");
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
	m_revision++;
	ScopedPtr<OutputStream> segmentsFile(m_dir->createFile(SegmentInfoList::segmentsFileName(m_revision)));
	m_infos.write(segmentsFile.get());
}

void IndexWriter::maybeFlush()
{
	if (m_segmentBuffer.size() > m_maxSegmentBufferSize) {
		flush();
	}
}

SegmentDataWriter *IndexWriter::segmentDataWriter(const SegmentInfo &info)
{
	OutputStream *indexOutput = m_dir->createFile(info.indexFileName());
	OutputStream *dataOutput = m_dir->createFile(info.dataFileName());
	SegmentIndexWriter *indexWriter = new SegmentIndexWriter(indexOutput);
	indexWriter->setBlockSize(BLOCK_SIZE);
	return new SegmentDataWriter(dataOutput, indexWriter, indexWriter->blockSize());
}

void IndexWriter::maybeMerge()
{
	QList<int> merge = m_mergePolicy->findMerges(m_infos);
	if (merge.isEmpty()) {
		return;
	}
	//qDebug() << "Merging segments" << merge;

	SegmentInfo info(m_infos.incLastSegmentId());
	{
		SegmentMerger merger(segmentDataWriter(info));
		for (size_t i = 0; i < merge.size(); i++) {
			int j = merge.at(i);
			merger.addSource(new SegmentEnum(segmentIndex(j), segmentDataReader(j)));
		}
		info.setBlockCount(merger.merge());
	}

	SegmentInfoList infos;
	infos.setLastSegmentId(m_infos.lastSegmentId());
	QSet<int> merged = merge.toSet();
	for (size_t i = 0; i < m_infos.size(); i++) {
		if (!merged.contains(i)) {
			infos.add(m_infos.info(i));
		}
		else {
			closeSegmentIndex(i);
		}
	}
	infos.add(info);

	m_infos = infos;
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
	for (size_t i = 0; i < m_segmentBuffer.size(); i++) {
		uint32_t key = (m_segmentBuffer[i] >> 32);
		uint32_t value = m_segmentBuffer[i] & 0xffffffff;
		//qDebug() << "adding item" << key << value;
		writer->addItem(key, value);
	}
	writer->close();
	info.setBlockCount(writer->blockCount());

	m_infos.add(info);
	maybeMerge();

	m_segmentBuffer.clear();
}

