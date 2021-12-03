// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "index.h"

#include "index_file_deleter.h"
#include "index_reader.h"
#include "index_writer.h"
#include "segment_data_reader.h"
#include "segment_index_reader.h"
#include "segment_searcher.h"
#include "store/directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"

using namespace Acoustid;

Index::Index(DirectorySharedPtr dir, bool create)
    : m_mutex(QMutex::Recursive), m_dir(dir), m_open(false), m_hasWriter(false), m_deleter(new IndexFileDeleter(dir)) {
    open(create);
}

Index::~Index() {}

bool Index::exists(const QSharedPointer<Directory> &dir) {
    if (!dir->exists()) {
        return false;
    }
    return IndexInfo::findCurrentRevision(dir.get()) >= 0;
}

void Index::open(bool create) {
    if (!m_dir->exists() && !create) {
        throw IOException("index directory does not exist");
    }
    if (!m_info.load(m_dir.data(), true, true)) {
        if (create) {
            m_dir->ensureExists();
            IndexWriter(m_dir, m_info).commit();
            return open(false);
        }
        throw IOException("there is no index in the directory");
    }
    m_deleter->incRef(m_info);
    m_open = true;
}

bool Index::isOpen() const { return m_open; }

void Index::acquireWriterLock() {
    QMutexLocker locker(&m_mutex);
    if (m_hasWriter) {
        throw IOException("there already is an index writer open");
    }
    m_hasWriter = true;
}

void Index::releaseWriterLock() {
    QMutexLocker locker(&m_mutex);
    m_hasWriter = false;
}

IndexInfo Index::acquireInfo() {
    QMutexLocker locker(&m_mutex);
    IndexInfo info = m_info;
    if (m_open) {
        m_deleter->incRef(info);
    }
    // qDebug() << "acquireInfo" << info.files();
    return info;
}

void Index::releaseInfo(const IndexInfo &info) {
    QMutexLocker locker(&m_mutex);
    if (m_open) {
        m_deleter->decRef(info);
    }
    // qDebug() << "releaseInfo" << info.files();
}

void Index::updateInfo(const IndexInfo &oldInfo, const IndexInfo &newInfo, bool updateIndex) {
    QMutexLocker locker(&m_mutex);
    if (m_open) {
        // the infos are opened twice (index + writer), so we need to inc/dec-ref them twice too
        m_deleter->incRef(newInfo);
        if (updateIndex) {
            m_deleter->incRef(newInfo);
            m_deleter->decRef(m_info);
        }
        m_deleter->decRef(oldInfo);
    }
    if (updateIndex) {
        m_info = newInfo;
        for (int i = 0; i < m_info.segmentCount(); i++) {
            assert(!m_info.segment(i).index().isNull());
        }
    }
}

bool Index::containsDocument(uint32_t docId) {
    IndexReader reader(sharedFromThis());
    return reader.containsDocument(docId);
}

std::vector<SearchResult> Index::search(const std::vector<uint32_t> &terms, int64_t timeoutInMSecs) {
    IndexReader reader(sharedFromThis());
    return reader.search(terms, timeoutInMSecs);
}

bool Index::hasAttribute(const QString &name) { return info().hasAttribute(name); }

QString Index::getAttribute(const QString &name) { return info().attribute(name); }

void Index::insertOrUpdateDocument(uint32_t docId, const std::vector<uint32_t> &terms) {
    OpBatch batch;
    batch.insertOrUpdateDocument(docId, terms);
    applyUpdates(batch);
}

void Index::deleteDocument(uint32_t docId) {
    OpBatch batch;
    batch.deleteDocument(docId);
    applyUpdates(batch);
}

void Index::applyUpdates(const OpBatch &batch) {
    IndexWriter writer(sharedFromThis());
    for (auto op : batch) {
        switch (op.type()) {
            case INSERT_OR_UPDATE_DOCUMENT: {
                auto data = std::get<InsertOrUpdateDocument>(op.data());
                writer.insertOrUpdateDocument(data.docId, data.terms);
            } break;
            case DELETE_DOCUMENT: {
                auto data = std::get<DeleteDocument>(op.data());
                writer.deleteDocument(data.docId);
            } break;
            case SET_ATTRIBUTE: {
                auto data = std::get<SetAttribute>(op.data());
                writer.setAttribute(data.name, data.value);
            } break;
        }
    }
    writer.commit();
}
