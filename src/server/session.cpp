// Copyright (C) 2020  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "session.h"
#include "errors.h"
#include "index/index.h"
#include "index/index_writer.h"

using namespace Acoustid;
using namespace Acoustid::Server;

void Session::begin() {
    QMutexLocker locker(&m_mutex);
    if (!m_indexWriter.isNull()) {
        throw AlreadyInTransactionException();
    }
    m_indexWriter = m_index->openWriter();
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
    return m_indexWriter->info().getAttribute(name);
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
    if (name == "trace_id") {
	m_traceId = value;
	return;
    }
    if (m_indexWriter.isNull()) {
        throw NotInTransactionException();
    }
    m_indexWriter->setAttribute(name, value);
}

void Session::insert(uint32_t id, const std::vector<uint32_t> &hashes) {
    QMutexLocker locker(&m_mutex);
    if (m_indexWriter.isNull()) {
        throw NotInTransactionException();
    }
    m_indexWriter->addDocument(id, hashes.data(), hashes.size());
}

std::vector<SearchResult> Session::search(const std::vector<uint32_t> &hashes) {
    QMutexLocker locker(&m_mutex);
    std::vector<SearchResult> results;
    try {
        results = m_index->search(hashes, m_timeout);
    } catch (TimeoutExceeded &ex) {
        throw HandlerException("timeout exceeded");
    }
    filterSearchResults(results, m_maxResults, m_topScorePercent);
    return results;
}

QString Session::getTraceId() {
    QMutexLocker locker(&m_mutex);
    return m_traceId;
}

void Session::clearTraceId() {
    QMutexLocker locker(&m_mutex);
    m_traceId.clear();
}
