// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "index_writer.h"

#include <algorithm>

#include "index.h"
#include "index_file_deleter.h"
#include "index_utils.h"
#include "segment_data_reader.h"
#include "segment_data_writer.h"
#include "segment_docs_writer.h"
#include "segment_index_reader.h"
#include "segment_index_writer.h"
#include "segment_merger.h"
#include "store/directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"

using namespace Acoustid;

IndexWriter::IndexWriter(DirectorySharedPtr dir, const IndexInfo& info)
    : IndexReader(dir, info), m_maxSegmentBufferSize(MAX_SEGMENT_BUFFER_SIZE), m_maxDocumentId(0) {
    m_mergePolicy.reset(new SegmentMergePolicy());
}

IndexWriter::IndexWriter(IndexSharedPtr index) : IndexReader(index), m_maxSegmentBufferSize(MAX_SEGMENT_BUFFER_SIZE), m_maxDocumentId(0) {
    m_index->acquireWriterLock();
    m_mergePolicy.reset(new SegmentMergePolicy());
}

IndexWriter::~IndexWriter() {
    if (m_index) {
        m_index->releaseWriterLock();
    }
}

void IndexWriter::insertOrUpdateDocument(uint32_t docId, const QVector<uint32_t>& terms) {
    m_segmentBufferDocs[docId] = false;
    for (auto term : terms) {
        m_segmentBuffer.push_back(packItem(term, docId));
    }
    if (docId > m_maxDocumentId) {
        m_maxDocumentId = docId;
    }
    maybeFlush();
}

void IndexWriter::deleteDocument(uint32_t docId) {
    m_segmentBufferDocs[docId] = true;
    maybeFlush();
}

void IndexWriter::setAttribute(const QString& name, const QString& value) { m_info.setAttribute(name, value); }

void IndexWriter::commit() {
    flush();
    IndexInfo info(m_info);
    info.save(m_dir.data());
    if (m_index) {
        m_index->updateInfo(m_info, info, true);
    }
    m_info = info;
    qDebug() << "Committed revision" << m_info.revision() << m_info.segments().size();
}

void IndexWriter::maybeFlush() {
    if (m_segmentBuffer.size() > m_maxSegmentBufferSize) {
        flush();
    }
}

SegmentDataWriter* IndexWriter::segmentDataWriter(const SegmentInfo& segment) {
    OutputStream* indexOutput = m_dir->createFile(segment.indexFileName());
    OutputStream* dataOutput = m_dir->createFile(segment.dataFileName());
    SegmentIndexWriter* indexWriter = new SegmentIndexWriter(indexOutput);
    return new SegmentDataWriter(dataOutput, indexWriter, BLOCK_SIZE);
}

void IndexWriter::merge(const QList<int>& merge) {
    if (merge.isEmpty()) {
        return;
    }

    const SegmentInfoList& segments = m_info.segments();

    IndexInfo info(m_info);
    SegmentInfo segment(info.incLastSegmentId());

    auto docs = std::make_shared<SegmentDocs>();
    {
        for (size_t i = 0; i < merge.size(); i++) {
            auto j = merge.at(i);
            const auto s = segments.at(j);
            for (auto doc : *s.docs()) {
                docs->add(doc.docId(), doc.version(), doc.isDeleted());
            }
        }
        saveSegmentDocs(segment, docs);
    }

    {
        SegmentMerger merger(segmentDataWriter(segment));
        for (size_t i = 0; i < merge.size(); i++) {
            int j = merge.at(i);
            const SegmentInfo& s = segments.at(j);
            QSet<uint32_t> excludeDocIds;
            for (auto doc : *s.docs()) {
                if (doc.version() < docs->getVersion(doc.docId())) {
                    qDebug() << "Need to remove" << doc.docId() << "from" << s.name() << "before merging";
                    excludeDocIds.insert(doc.docId());
                }
            }
            qDebug() << "Merging segment" << s.id() << "with checksum" << s.checksum() << "into segment" << segment.id();
            auto source = new SegmentEnum(s.index(), segmentDataReader(s));
            source->setFilter(excludeDocIds);
            merger.addSource(source);
        }
        merger.merge();
        segment.setBlockCount(merger.writer()->blockCount());
        segment.setLastKey(merger.writer()->lastKey());
        segment.setChecksum(merger.writer()->checksum());
        segment.setIndex(merger.writer()->index());
    }

    qDebug() << "New segment" << segment.id() << "with checksum" << segment.checksum() << "(merge)";

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

void IndexWriter::maybeMerge() {
    const SegmentInfoList& segments = m_info.segments();
    merge(m_mergePolicy->findMerges(segments));
}

void IndexWriter::saveSegmentDocs(SegmentInfo& segment, const std::shared_ptr<SegmentDocs>& docs) {
    auto output = std::unique_ptr<OutputStream>(m_dir->createFile(segment.docsFileName()));
    writeSegmentDocs(output.get(), docs.get());
    segment.setDocs(docs);
}

void IndexWriter::flush() {
    if (m_segmentBuffer.empty() && m_segmentBufferDocs.empty()) {
        return;
    }
    // qDebug() << "Writing new segment" << (m_segmentBuffer.size() * 8.0 / 1024
    // / 1024);

    IndexInfo info(m_info);
    SegmentInfo segment(info.incLastSegmentId());

    {
        std::unique_ptr<SegmentDataWriter> writer(segmentDataWriter(segment));
        uint64_t lastItem = UINT64_MAX;
        std::sort(m_segmentBuffer.begin(), m_segmentBuffer.end());
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

    {
        auto docs = std::make_shared<SegmentDocs>();
        const auto version = segment.id();
        for (auto it : m_segmentBufferDocs) {
            const auto docId = it.first;
            const auto isDeleted = it.second;
            docs->add(docId, version, isDeleted);
        }
        saveSegmentDocs(segment, docs);
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
    m_segmentBufferDocs.clear();
}

void IndexWriter::optimize() {
    flush();

    const SegmentInfoList& segments = m_info.segments();
    QList<int> merges;
    for (int i = 0; i < segments.size(); i++) {
        merges.append(i);
    }
    merge(merges);
}

void IndexWriter::cleanup() {
    flush();

    QSet<QString> usedFileNames;
    usedFileNames.insert(m_info.indexInfoFileName(m_info.revision()));
    const SegmentInfoList& segments = m_info.segments();
    for (int i = 0; i < segments.size(); i++) {
        const SegmentInfo& segment = segments.at(i);
        usedFileNames.insert(segment.indexFileName());
        usedFileNames.insert(segment.dataFileName());
        usedFileNames.insert(segment.docsFileName());
    }

    QList<QString> allFileNames = m_dir->listFiles();
    for (int i = 0; i < allFileNames.size(); i++) {
        if (!usedFileNames.contains(allFileNames.at(i))) {
            m_dir->deleteFile(allFileNames.at(i));
        }
    }
}
