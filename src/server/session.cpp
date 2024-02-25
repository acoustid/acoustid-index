// Copyright (C) 2020  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "session.h"

#include "errors.h"
#include "index/index.h"
#include "index/op.h"

using namespace Acoustid;
using namespace Acoustid::Server;

void Session::begin() {
    QMutexLocker locker(&m_mutex);
    if (m_transaction) {
        throw AlreadyInTransactionException();
    }
    m_transaction = std::make_unique<OpBatch>();
}

void Session::commit() {
    QMutexLocker locker(&m_mutex);
    if (!m_transaction) {
        throw NotInTransactionException();
    }
    m_index->applyUpdates(*m_transaction);
    m_transaction.reset();
}

void Session::rollback() {
    QMutexLocker locker(&m_mutex);
    if (!m_transaction) {
        throw NotInTransactionException();
    }
    m_transaction.reset();
}

void Session::optimize() {}

void Session::cleanup() {}

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
    auto value = m_index->getAttribute(name);
    if (m_transaction) {
        value = m_transaction->getAttribute(name, value);
    }
    return value;
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
    if (!m_transaction) {
        throw NotInTransactionException();
    }
    m_transaction->setAttribute(name, value);
}

void Session::insertOrUpdateDocument(uint32_t id, const std::vector<uint32_t> &terms) {
    QMutexLocker locker(&m_mutex);
    if (!m_transaction) {
        throw NotInTransactionException();
    }
    m_transaction->insertOrUpdateDocument(id, terms);
}

void Session::deleteDocument(uint32_t id) {
    QMutexLocker locker(&m_mutex);
    if (!m_transaction) {
        throw NotInTransactionException();
    }
    m_transaction->deleteDocument(id);
}

std::vector<SearchResult> Session::search(const std::vector<uint32_t> &terms) {
    QMutexLocker locker(&m_mutex);
    std::vector<SearchResult> results;
    try {
        results = m_index->search(terms, m_timeout);
    } catch (const TimeoutExceeded &e) {
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
