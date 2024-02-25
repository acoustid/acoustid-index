// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "index_writer.h"

#include <algorithm>

#include "in_memory_index.h"
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

IndexWriter::IndexWriter(IndexSharedPtr index, bool alreadyHasLock) : IndexReader(index) {
    if (!alreadyHasLock) {
        m_index->acquireWriterLock();
    }
    m_mergePolicy = std::make_unique<SegmentMergePolicy>();
    m_info.incRevision();
}

IndexWriter::~IndexWriter() {
    if (m_index) {
        m_index->releaseWriterLock();
    }
}

void IndexWriter::commit() {
    maybeMerge();
    IndexInfo info(m_info);
    info.save(m_dir.data());
    if (m_index) {
        m_index->updateInfo(m_info, info, true);
    }
    m_info = info;
    m_info.incRevision();
    qDebug() << "Committed revision" << m_info.revision() << m_info.segments().size();
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
        for (int i = 0; i < merge.size(); i++) {
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
        for (int i = 0; i < merge.size(); i++) {
            int j = merge.at(i);
            const SegmentInfo& s = segments.at(j);
            QSet<uint32_t> excludeDocIds;
            for (auto doc : *s.docs()) {
                if (doc.version() < docs->getVersion(doc.docId())) {
                    excludeDocIds.insert(doc.docId());
                }
            }
            qDebug() << "Merging segment" << s.id() << "with checksum" << s.checksum() << "into segment"
                     << segment.id();
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

    QSet<int> merged(merge.begin(), merge.end());
    info.clearSegments();
    for (int i = 0; i < segments.size(); i++) {
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

void IndexWriter::optimize() {
    const SegmentInfoList& segments = m_info.segments();
    QList<int> merges;
    for (int i = 0; i < segments.size(); i++) {
        merges.append(i);
    }
    merge(merges);
}

void IndexWriter::cleanup() {
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

void IndexWriter::writeSegment(const std::shared_ptr<InMemoryIndex>& index) {
    auto snapshot = index->snapshot();

    IndexInfo info(m_info);
    SegmentInfo segment(info.incLastSegmentId());
    qDebug() << "Building segment" << segment.id();

    auto attrs = snapshot.attributes();
    for (auto it = attrs.begin(); it != attrs.end(); ++it) {
        info.setAttribute(it.key(), it.value());
        qDebug() << "Setting attribute" << it.key() << "to" << it.value();
    }

    auto segmentDocs = std::make_shared<SegmentDocs>();
    for (auto doc : snapshot.docs()) {
        segmentDocs->add(doc.id(), info.revision(), doc.isDeleted());
    }
    saveSegmentDocs(segment, segmentDocs);

    std::unique_ptr<SegmentDataWriter> writer(segmentDataWriter(segment));
    auto terms = snapshot.terms();
    for (auto it = terms.begin(); it != terms.end(); ++it) {
        writer->addItem(it.term(), it.docId());
    }
    writer->close();

    segment.setBlockCount(writer->blockCount());
    segment.setLastKey(writer->lastKey());
    segment.setChecksum(writer->checksum());
    segment.setIndex(writer->index());

    qDebug() << "Built new segment" << segment.id() << "with checksum" << segment.checksum();
    info.addSegment(segment);

    if (m_index) {
        m_index->updateInfo(m_info, info, false);
    }
    m_info = info;
}
