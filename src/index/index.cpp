// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "index.h"

#include <QtConcurrent>

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

using namespace Acoustid;

template <typename Idx>
int64_t getLastOpId(const Idx &idx) {
    return idx->getAttribute("status.last_op_id").toLongLong();
}

template <typename Idx>
void setLastOpId(Idx &idx, int64_t id) {
    idx->setAttribute("status.last_op_id", QString::number(id));
}

Index::Index(DirectorySharedPtr dir, bool create)
    : m_lock(QReadWriteLock::Recursive), m_dir(dir), m_deleter(new IndexFileDeleter(dir)) {
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

void Index::setThreadPool(QThreadPool *threadPool) { m_threadPool = threadPool; }

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

    if (!m_dir->exists() && !create) {
        throw IndexNotFoundException("index directory does not exist");
    }
    if (!m_info.load(m_dir.data(), true, true)) {
        if (create) {
            qDebug() << "Creating new index" << m_dir->path();
            m_dir->ensureExists();
            m_info.incRevision();
            m_info.save(m_dir.data());
            locker.unlock();
            return open(false);
        }
        throw IndexNotFoundException("there is no index in the directory");
    }

    qDebug() << "Opening index" << m_dir->path();

    m_oplog = std::make_unique<Oplog>(m_dir->openDatabase("oplog.db"));

    auto stage = std::make_shared<InMemoryIndex>();
    stage->setRevision(m_info.revision() + 1);
    qDebug() << "Creating new staging area" << stage->revision();

    m_stage.push_back(stage);

    auto lastOpId = getLastOpId(&m_info);
    m_oplog->createOrUpdateReplicationSlot("main", lastOpId);

    std::vector<OplogEntry> oplogEntries;
    while (true) {
        oplogEntries.clear();
        lastOpId = m_oplog->read(oplogEntries, 100, lastOpId);
        if (oplogEntries.empty()) {
            break;
        }
        OpBatch batch;
        for (auto oplogEntry : oplogEntries) {
            qDebug() << "Applying oplog entry" << oplogEntry.id();
            batch.add(oplogEntry.op());
        }
        stage->applyUpdates(batch);
    }

    m_deleter->incRef(m_info);
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
        for (size_t i = 0; i < newInfo.segmentCount(); i++) {
            assert(!newInfo.segment(i).index().isNull());
            assert(newInfo.segment(i).docs());
        }
        m_info = newInfo;
        for (auto it = m_stage.begin(); it != m_stage.end(); ++it) {
            if ((*it)->revision() == m_info.revision()) {
                qDebug() << "Closing staging area" << (*it)->revision();
                m_stage.erase(it);
                break;
            }
        }
    }
}

bool Index::containsDocument(uint32_t docId) {
    QReadLocker locker(&m_lock);
    if (!m_open) {
        throw IndexIsNotOpen("index is not open");
    }
    for (auto stage : m_stage) {
        bool isDeleted;
        if (stage->getDocument(docId, isDeleted)) {
            return !isDeleted;
        }
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
    return openWriterPrivate(wait, QDeadlineTimer(timeoutInMSecs));
}

QSharedPointer<IndexReader> Index::openReaderPrivate() { return QSharedPointer<IndexReader>::create(sharedFromThis()); }

QSharedPointer<IndexWriter> Index::openWriterPrivate(bool wait, QDeadlineTimer deadline) {
    acquireWriterLockPrivate(wait, deadline);
    return QSharedPointer<IndexWriter>::create(sharedFromThis(), true);
}

void Index::acquireWriterLockPrivate(bool wait, QDeadlineTimer deadline) {
    if (m_hasWriter) {
        if (wait) {
            if (m_writerReleased.wait(&m_lock, deadline)) {
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
    acquireWriterLockPrivate(wait, QDeadlineTimer(timeoutInMSecs));
}

void Index::releaseWriterLock() {
    QWriteLocker locker(&m_lock);
    m_hasWriter = false;
    m_writerReleased.notify_one();
}

std::vector<SearchResult> Index::search(const std::vector<uint32_t> &terms, int64_t timeoutInMSecs) {
    QReadLocker locker(&m_lock);
    QDeadlineTimer deadline(timeoutInMSecs);

    std::vector<SearchResult> results;

    for (auto it = m_stage.begin(); it != m_stage.end(); ++it) {
        qDebug() << "Searching in staging area" << (*it)->revision();
        auto partialResults = (*it)->search(terms, deadline.remainingTime());
        for (auto result : partialResults) {
            bool foundNewer = false;
            for (auto it2 = m_stage.begin(); it2 != it; ++it2) {
                bool isDeleted;
                if ((*it2)->getDocument(result.docId(), isDeleted)) {
                    foundNewer = true;
                    break;
                }
            }
            if (!foundNewer) {
                results.push_back(result);
            }
        }
    }

    qDebug() << "Searching in main index";
    auto partialResults = openReaderPrivate()->search(terms, deadline.remainingTime());
    for (auto result : partialResults) {
        bool foundNewer = false;
        for (auto it2 = m_stage.begin(); it2 != m_stage.end(); ++it2) {
            bool isDeleted;
            if ((*it2)->getDocument(result.docId(), isDeleted)) {
                foundNewer = true;
                break;
            }
        }
        if (!foundNewer) {
            results.push_back(result);
        }
    }

    sortSearchResults(results);
    return results;
}

bool Index::hasAttribute(const QString &name) {
    QReadLocker locker(&m_lock);
    for (auto stage : m_stage) {
        if (stage->hasAttribute(name)) {
            return true;
        }
    }
    return info().hasAttribute(name);
}

QString Index::getAttribute(const QString &name) {
    QReadLocker locker(&m_lock);
    for (auto stage : m_stage) {
        if (stage->hasAttribute(name)) {
            return stage->getAttribute(name);
        }
    }
    return info().getAttribute(name);
}

void Index::setAttribute(const QString &name, const QString &value) {
    OpBatch batch;
    batch.setAttribute(name, value);
    applyUpdates(batch);
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

void Index::persistUpdates(const std::shared_ptr<InMemoryIndex> &index) {
    auto lastOpId = getLastOpId(index);

    qDebug() << "Persisting operations up to" << getLastOpId(index) << "from staging area" << index->revision();

    auto writer = openWriter(true, -1);
    writer->writeSegment(index);
    writer->commit();

    m_oplog->updateReplicationSlot("main", lastOpId);
    m_oplog->cleanup();
}

void Index::persistUpdates() {
    QWriteLocker locker(&m_lock);
    while (m_stage.size() > 1) {
        auto stage = m_stage.back();
        locker.unlock();
        persistUpdates(stage);
        locker.relock();
    }
}

void Index::applyUpdates(const OpBatch &batch) {
    QWriteLocker locker(&m_lock);

    // Store the updates in oplog
    auto lastOpId = m_oplog->write(batch);

    // Apply the updates to the staging area
    auto stage = m_stage.front();
    qDebug() << "Applying" << batch.size() << "updates to staging area" << stage->revision();
    stage->applyUpdates(batch);
    setLastOpId(stage, lastOpId);

    if (stage->size() > m_maxStageSize) {
        flush();
    }
}

void Index::flush() {
    QWriteLocker locker(&m_lock);

    auto stage = m_stage.front();
    if (stage->size() == 0) {
        return;
    }

    auto nextStage = std::make_shared<InMemoryIndex>();
    nextStage->setRevision(stage->revision() + 1);
    qDebug() << "Creating new staging area" << nextStage->revision();
    m_stage.insert(m_stage.begin(), nextStage);

    if (m_threadPool) {
        if (!m_writerFuture.isRunning()) {
            auto self = sharedFromThis();
            m_writerFuture = QtConcurrent::run(m_threadPool, [=]() {
                self->persistUpdates();
            });
        }
    } else {
        locker.unlock();
        persistUpdates();
        locker.relock();
    }
}

bool Index::hasAttribute(const QString &name) {
    QMutexLocker locker(&m_mutex);
    return info().hasAttribute(name);
}

QString Index::getAttribute(const QString &name) {
    QMutexLocker locker(&m_mutex);
    return info().getAttribute(name);
}

void Index::setAttribute(const QString &name, const QString &value) {
    OpBatch batch;
    batch.setAttribute(name, value);
    applyUpdates(batch);
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

void Index::applyUpdates(const OpBatch &batch) {
    auto writer = openWriter(true);
    for (const auto &op : batch) {
        switch (op.type()) {
            case INSERT_OR_UPDATE_DOCUMENT: {
                auto data = op.data<InsertOrUpdateDocument>();
                writer->addDocument(data.docId, data.terms.data(), data.terms.size());
                break;
            }
            case DELETE_DOCUMENT: {
                throw NotImplemented("Document deletion is not implemented");
            }
            case SET_ATTRIBUTE: {
                auto data = op.data<SetAttribute>();
                writer->setAttribute(data.name, data.value);
                break;
            }
        }
    }

}

std::vector<SearchResult> Index::search(const std::vector<uint32_t> &terms, int64_t timeoutInMSecs) {
    auto reader = openReader();
    return reader->search(terms.data(), terms.size(), timeoutInMSecs);
}
