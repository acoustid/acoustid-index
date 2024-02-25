// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "store/directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "segment_index_reader.h"
#include "segment_data_reader.h"
#include "segment_searcher.h"
#include "index_file_deleter.h"
#include "index_reader.h"
#include "index_writer.h"
#include "index.h"

using namespace Acoustid;

Index::Index(DirectorySharedPtr dir, bool create)
	: m_mutex(), m_dir(dir), m_open(false),
	  m_hasWriter(false),
	  m_deleter(new IndexFileDeleter(dir))
{
	open(create);
}

Index::~Index()
{
}

bool Index::containsDocument(uint32_t docId) {
    return true;
}

bool Index::exists(const QSharedPointer<Directory> &dir) {
    return IndexInfo::findCurrentRevision(dir.get()) >= 0;
}

void Index::open(bool create)
{
	QMutexLocker locker(&m_mutex);
	if (!m_info.load(m_dir.data(), true)) {
		if (create) {
			IndexWriter(m_dir, m_info).commit();
			locker.unlock();
			return open(false);
	 	}
		throw IndexNotFoundException("index directory does not exist");
	}
	m_deleter->incRef(m_info);
	m_open = true;
}

QSharedPointer<IndexReader> Index::openReader()
{
    QMutexLocker locker(&m_mutex);
    if (!m_open) {
       throw IndexIsNotOpen("index is not open");
    }
    locker.unlock();
    return QSharedPointer<IndexReader>::create(sharedFromThis());
}

QSharedPointer<IndexWriter> Index::openWriter(bool wait, int64_t timeoutInMSecs)
{
    QMutexLocker locker(&m_mutex);
    if (!m_open) {
        throw IndexIsNotOpen("index is not open");
    }
    acquireWriterLockInt(wait, timeoutInMSecs);
    locker.unlock();
    return QSharedPointer<IndexWriter>::create(sharedFromThis(), true);
}

void Index::acquireWriterLockInt(bool wait, int64_t timeoutInMSecs)
{
	if (m_hasWriter) {
        if (wait) {
            if (m_writerReleased.wait(&m_mutex, QDeadlineTimer(timeoutInMSecs))) {
                m_hasWriter = true;
                return;
            }
        }
        throw IndexIsLocked("there already is an index writer open");
	}
	m_hasWriter = true;
}

void Index::acquireWriterLock(bool wait, int64_t timeoutInMSecs)
{
	QMutexLocker locker(&m_mutex);
    acquireWriterLockInt(wait, timeoutInMSecs);
}

void Index::releaseWriterLock()
{
	QMutexLocker locker(&m_mutex);
	m_hasWriter = false;
    m_writerReleased.notify_one();
}

IndexInfo Index::acquireInfo()
{
	QMutexLocker locker(&m_mutex);
	IndexInfo info = m_info;
	if (m_open) {
		m_deleter->incRef(info);
	}
	//qDebug() << "acquireInfo" << info.files();
	return info;
}

void Index::releaseInfo(const IndexInfo& info)
{
	QMutexLocker locker(&m_mutex);
	if (m_open) {
		m_deleter->decRef(info);
	}
	//qDebug() << "releaseInfo" << info.files();
}

void Index::updateInfo(const IndexInfo& oldInfo, const IndexInfo& newInfo, bool updateIndex)
{
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
    writer->commit();
}

std::vector<SearchResult> Index::search(const std::vector<uint32_t> &hashes, int64_t timeoutInMSecs) {
    auto reader = openReader();
    return reader->search(hashes, timeoutInMSecs);
}

void Index::flush() {
}
