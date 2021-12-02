// Copyright (C) 2020  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "session.h"
#include "errors.h"
#include "index/index_writer.h"

using namespace Acoustid;
using namespace Acoustid::Server;

void Session::begin() {
    QMutexLocker locker(&m_mutex);
    if (!m_indexWriter.isNull()) {
        throw AlreadyInTransactionException();
    }
    m_indexWriter = QSharedPointer<IndexWriter>::create(m_index);
}

void Session::commit() {
    QMutexLocker locker(&m_mutex);
    if (m_indexWriter.isNull()) {
        throw NotInTransactionException();
    }
    m_indexWriter->commit();
    m_indexWriter.clear();
}

void Session::rollback() {
    QMutexLocker locker(&m_mutex);
    if (m_indexWriter.isNull()) {
        throw NotInTransactionException();
    }
    m_indexWriter.clear();
}

void Session::optimize() {
    QMutexLocker locker(&m_mutex);
    if (m_indexWriter.isNull()) {
        throw NotInTransactionException();
    }
    m_indexWriter->optimize();
}

void Session::cleanup() {
    QMutexLocker locker(&m_mutex);
    if (m_indexWriter.isNull()) {
        throw NotInTransactionException();
    }
    m_indexWriter->cleanup();
}

QString Session::getAttribute(const QString &name) {
    QMutexLocker locker(&m_mutex);
    if (name == "max_results") {
        return QString("%1").arg(m_maxResults);
    }
    if (name == "top_score_percent") {
        return QString("%1").arg(m_topScorePercent);
    }
    if (name == "timeout") {
        return QString("%1").arg(m_timeout);
    }
    if (name == "idle_timeout") {
        return QString("%1").arg(m_idle_timeout);
    }
    if (m_indexWriter.isNull()) {
        return m_index->getAttribute(name);
    }
    return m_indexWriter->info().attribute(name);
}

void Session::setAttribute(const QString &name, const QString &value) {
    QMutexLocker locker(&m_mutex);
    if (name == "max_results") {
        m_maxResults = value.toInt();
        return;
    }
    if (name == "top_score_percent") {
        m_topScorePercent = value.toInt();
        return;
    }
    if (name == "timeout") {
        m_timeout = value.toInt();
        return;
    }
    if (name == "idle_timeout") {
        m_idle_timeout = value.toInt();
        return;
    }
    if (m_indexWriter.isNull()) {
        throw NotInTransactionException();
    }
    m_indexWriter->setAttribute(name, value);
}

void Session::insertOrUpdateDocument(uint32_t id, const std::vector<uint32_t> &terms) {
    QMutexLocker locker(&m_mutex);
    if (m_indexWriter.isNull()) {
        throw NotInTransactionException();
    }
    m_indexWriter->insertOrUpdateDocument(id, terms);
}

void Session::deleteDocument(uint32_t id) {
    QMutexLocker locker(&m_mutex);
    if (m_indexWriter.isNull()) {
        throw NotInTransactionException();
    }
    m_indexWriter->deleteDocument(id);
}

std::vector<SearchResult> Session::search(const std::vector<uint32_t> &terms) {
    QMutexLocker locker(&m_mutex);
    std::vector<SearchResult> results;
    try {
        results = m_index->search(terms, m_timeout);
    } catch (TimeoutExceeded) {
        throw HandlerException("timeout exceeded");
    }
    filterSearchResults(results, m_maxResults, m_topScorePercent);
    return results;
}
