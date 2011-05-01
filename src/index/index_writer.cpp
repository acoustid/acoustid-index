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

#define BLOCK_SIZE 512

using namespace Acoustid;

IndexWriter::IndexWriter(Directory *dir, bool create)
	: m_dir(dir), m_numDocsInBuffer(0), m_maxSegmentBufferSize(1024 * 1024 * 10)
{
	m_mergePolicy = new SegmentMergePolicy();
	m_revision = SegmentInfoList::findCurrentRevision(dir);
	if (m_revision != -1) {
		m_segmentInfos.read(dir->openFile(SegmentInfoList::segmentsFileName(m_revision)));
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
}

int IndexWriter::revision()
{
	return m_revision;
}

void IndexWriter::addDocument(uint32_t id, uint32_t *terms, size_t length)
{
	for (size_t i = 0; i < length; i++) {
		m_segmentBuffer.push_back((uint64_t(terms[i]) << 32) | id);
	}
	m_numDocsInBuffer++;
	maybeFlush();
}

void IndexWriter::commit()
{
	flush();
	m_revision++;
	ScopedPtr<OutputStream> segmentsFile(m_dir->createFile(SegmentInfoList::segmentsFileName(m_revision)));
	m_segmentInfos.write(segmentsFile.get());
}

void IndexWriter::maybeFlush()
{
	if (m_segmentBuffer.size() > m_maxSegmentBufferSize) {
		flush();
	}
}

void IndexWriter::maybeMerge()
{
	QList<int> merge = m_mergePolicy->findMerges(m_segmentInfos);
	if (merge.isEmpty()) {
		return;
	}

	SegmentInfo info(m_segmentInfos.incNextSegmentId());
	ScopedPtr<OutputStream> indexOutput(m_dir->createFile(info.name() + ".fii"));
	ScopedPtr<OutputStream> dataOutput(m_dir->createFile(info.name() + ".fid"));

	SegmentIndexWriter indexWriter(indexOutput.get());
	indexWriter.setBlockSize(BLOCK_SIZE);

	SegmentDataWriter *writer = new SegmentDataWriter(dataOutput.get(), &indexWriter, indexWriter.blockSize());
	SegmentMerger merger(writer);
	//qDebug() << "merging";
	for (size_t i = 0; i < merge.size(); i++) {
		SegmentInfo mergeInfo = m_segmentInfos.info(merge.at(i));
		SegmentIndex *index = SegmentIndexReader(m_dir->openFile(mergeInfo.name() + ".fii")).read();
		SegmentDataReader *dataReader = new SegmentDataReader(m_dir->openFile(mergeInfo.name() + ".fid"), BLOCK_SIZE);
		SegmentEnum *segmentEnum = new SegmentEnum(index, dataReader);
		//qDebug() << "adding source" << i << mergeInfo.id() << mergeInfo.numDocs();
		merger.addSource(segmentEnum);
	}
	info.setNumDocs(merger.merge());

	SegmentInfoList infos;
	infos.setNextSegmentId(m_segmentInfos.lastSegmetNum());
	QSet<int> merged = merge.toSet();
	//qDebug() << "merge" << merge;
	//qDebug() << "merged" << merged;
	for (size_t i = 0; i < m_segmentInfos.size(); i++) {
		if (!merged.contains(i)) {
			infos.add(m_segmentInfos.info(i));
		}
	}
	infos.add(info);

	m_segmentInfos = infos;
}

void IndexWriter::flush()
{
	if (m_segmentBuffer.empty()) {
		return;
	}
	std::sort(m_segmentBuffer.begin(), m_segmentBuffer.end());

	SegmentInfo info(m_segmentInfos.incNextSegmentId(), m_numDocsInBuffer);
	//qDebug() << "new segment" << info.id() << m_numDocsInBuffer;
	ScopedPtr<OutputStream> indexOutput(m_dir->createFile(info.name() + ".fii"));
	ScopedPtr<OutputStream> dataOutput(m_dir->createFile(info.name() + ".fid"));

	SegmentIndexWriter indexWriter(indexOutput.get());
	indexWriter.setBlockSize(BLOCK_SIZE);

	SegmentDataWriter writer(dataOutput.get(), &indexWriter, indexWriter.blockSize());
	for (size_t i = 0; i < m_segmentBuffer.size(); i++) {
		uint32_t key = (m_segmentBuffer[i] >> 32);
		uint32_t value = m_segmentBuffer[i] & 0xffffffff;
		writer.addItem(key, value);
	}
	writer.close();

	m_segmentInfos.add(info);
	//qDebug() << "segments before merge:";
	//for (size_t i = 0; i < m_segmentInfos.size(); i++) {
		//qDebug() << "   id =" << m_segmentInfos.info(i).id() << m_segmentInfos.info(i).numDocs();
	//}
	maybeMerge();
	//qDebug() << "segments after merge:";
	//for (size_t i = 0; i < m_segmentInfos.size(); i++) {
		//qDebug() << "   id =" << m_segmentInfos.info(i).id() << m_segmentInfos.info(i).numDocs();
	//}

	m_segmentBuffer.clear();
	m_numDocsInBuffer = 0;
}

