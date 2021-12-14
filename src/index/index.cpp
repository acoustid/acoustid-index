// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "index.h"

#include "in_memory_index.h"
#include "index/oplog.h"
#include "index_file_deleter.h"
#include "index_reader.h"
#include "index_writer.h"
#include "segment_data_reader.h"
#include "segment_index_reader.h"
#include "segment_searcher.h"
#include "store/directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"

#include <QtConcurrent>

using namespace Acoustid;

Index::Index(DirectorySharedPtr dir, bool create)
    : m_lock(QReadWriteLock::Recursive), m_dir(dir), m_open(false), m_deleter(new IndexFileDeleter(dir)) {
    open(create);
}

Index::~Index() { close(); }

void Index::close() {
    QWriteLocker locker(&m_lock);
    if (m_open) {
        qDebug() << "Closing index";
        m_writerFuture.waitForFinished();
        setThreadPool(nullptr);
        m_open = false;
    }
}

QThreadPool *Index::threadPool() const { return m_threadPool; }

void Index::setThreadPool(QThreadPool *threadPool) {
    if (threadPool != m_threadPool) {
        m_threadPool = threadPool;
    }
}

bool Index::exists(const QSharedPointer<Directory> &dir) {
    if (!dir->exists()) {
        return false;
    }
    return IndexInfo::findCurrentRevision(dir.get()) >= 0;
}

void Index::open(bool create) {
    QWriteLocker locker(&m_lock);

    if (m_open) {
        return;
    }

    qDebug() << "Opening index";

    if (!m_dir->exists() && !create) {
        throw IndexNotFoundException("index directory does not exist");
    }
    if (!m_info.load(m_dir.data(), true, true)) {
        if (create) {
            m_dir->ensureExists();
            IndexWriter(m_dir, m_info).commit();
            return open(false);
        }
        throw IndexNotFoundException("there is no index in the directory");
    }
    m_oplog = std::make_unique<OpLog>(m_dir->openDatabase("oplog.db"));
    m_stage = std::make_shared<InMemoryIndex>();

    std::vector<OpLogEntry> oplogEntries;
    auto lastOplogId = m_info.attribute("last_oplog_id").toInt();
    while (true) {
        oplogEntries.clear();
        lastOplogId = m_oplog->read(oplogEntries, 100, lastOplogId);
        if (oplogEntries.empty()) {
            break;
        }
        OpBatch batch;
        for (auto oplogEntry : oplogEntries) {
            qDebug() << "Applying oplog entry" << oplogEntry.id();
            batch.add(oplogEntry.op());
        }
        m_stage->applyUpdates(batch);
    }

    m_deleter->incRef(m_info);
    setThreadPool(QThreadPool::globalInstance());
    m_open = true;

    qDebug() << "Index opened";
}

bool Index::isOpen() {
    QReadLocker locker(&m_lock);
    return m_open;
}

IndexInfo Index::acquireInfo() {
    QReadLocker locker(&m_lock);
    IndexInfo info = m_info;
    if (m_open) {
        m_deleter->incRef(info);
    }
    // qDebug() << "acquireInfo" << info.files();
    return info;
}

void Index::releaseInfo(const IndexInfo &info) {
    QReadLocker locker(&m_lock);
    if (m_open) {
        m_deleter->decRef(info);
    }
    // qDebug() << "releaseInfo" << info.files();
}

void Index::updateInfo(const IndexInfo &oldInfo, const IndexInfo &newInfo, bool updateIndex) {
    QWriteLocker locker(&m_lock);
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
    QReadLocker locker(&m_lock);
    if (!m_open) {
        throw IndexIsNotOpen("index is not open");
    }
    bool isDeleted;
    if (m_stage->getDocument(docId, isDeleted)) {
        return !isDeleted;
    }
    return openReaderPrivate()->containsDocument(docId);
}

QSharedPointer<IndexReader> Index::openReader() {
    QReadLocker locker(&m_lock);
    if (!m_open) {
        throw IndexIsNotOpen("index is not open");
    }
    return openReaderPrivate();
}

QSharedPointer<IndexWriter> Index::openWriter(bool wait, int64_t timeoutInMSecs) {
    QReadLocker locker(&m_lock);
    if (!m_open) {
        throw IndexIsNotOpen("index is not open");
    }
    return openWriterPrivate(wait, timeoutInMSecs);
}

QSharedPointer<IndexReader> Index::openReaderPrivate() {
    return QSharedPointer<IndexReader>::create(sharedFromThis());
}

QSharedPointer<IndexWriter> Index::openWriterPrivate(bool wait, int64_t timeoutInMSecs) {
    acquireWriterLockPrivate(wait, timeoutInMSecs);
    return QSharedPointer<IndexWriter>::create(sharedFromThis(), true);
}

void Index::acquireWriterLockPrivate(bool wait, int64_t timeoutInMSecs) {
    if (m_hasWriter) {
        if (wait) {
            if (m_writerReleased.wait(&m_lock, QDeadlineTimer(timeoutInMSecs))) {
                m_hasWriter = true;
                return;
            }
        }
        throw IndexIsLocked("there already is an index writer open");
    }
    m_hasWriter = true;
}

void Index::acquireWriterLock(bool wait, int64_t timeoutInMSecs) {
    QWriteLocker locker(&m_lock);
    acquireWriterLockPrivate(wait, timeoutInMSecs);
}

void Index::releaseWriterLock() {
    QWriteLocker locker(&m_lock);
    m_hasWriter = false;
    m_writerReleased.notify_one();
}

std::vector<SearchResult> Index::search(const std::vector<uint32_t> &terms, int64_t timeoutInMSecs) {
    QReadLocker locker(&m_lock);
    QDeadlineTimer deadline(timeoutInMSecs);
    auto results = m_stage->search(terms, deadline.remainingTime());
    if (deadline.hasExpired()) {
        return results;
    }
    auto results2 = openReaderPrivate()->search(terms, deadline.remainingTime());
    for (auto result : results2) {
        bool isDeleted;
        if (!m_stage->getDocument(result.docId(), isDeleted)) {
            results.push_back(result);
        }
    }
    sortSearchResults(results);
    return results;
}

bool Index::hasAttribute(const QString &name) {
    QReadLocker locker(&m_lock);
    if (m_stage->hasAttribute(name)) {
        return true;
    }
    return info().hasAttribute(name);
}

QString Index::getAttribute(const QString &name) {
    QReadLocker locker(&m_lock);
    if (m_stage->hasAttribute(name)) {
        return m_stage->getAttribute(name);
    }
    return info().attribute(name);
}

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

void Index::persistUpdates() {
    qDebug() << "IndexWriter thread started";
    QThread::sleep(1);
}

void Index::applyUpdates(const OpBatch &batch) {
    QWriteLocker locker(&m_lock);
    m_oplog->write(batch);
    m_stage->applyUpdates(batch);

    if (m_stage->size() > 100000) {
        if (!m_writerFuture.isRunning()) {
            auto sharedThis = sharedFromThis();
            m_writerFuture = QtConcurrent::run(m_threadPool, [=]() {
                sharedThis->persistUpdates();
            });
        }
    }

    /*
    IndexWriter writer(sharedFromThis());
    for (auto op : batch) {
        switch (op.type()) {
            case INSERT_OR_UPDATE_DOCUMENT: {
                auto data = op.data<InsertOrUpdateDocument>();
                writer.insertOrUpdateDocument(data.docId, data.terms);
                break;
            }
            case DELETE_DOCUMENT: {
                auto data = op.data<DeleteDocument>();
                writer.deleteDocument(data.docId);
                break;
            }
            case SET_ATTRIBUTE: {
                auto data = op.data<SetAttribute>();
                writer.setAttribute(data.name, data.value);
                break;
            }
        }
    }
    writer.commit();
    */
}
