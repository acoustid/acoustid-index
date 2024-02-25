// Copyright (C) 2020  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SERVER_SESSION_H_
#define ACOUSTID_SERVER_SESSION_H_

#include <QMutex>
#include <QSharedPointer>
#include "index/search_result.h"

namespace Acoustid {

class Index;
class IndexWriter;

namespace Server {

class Metrics;

class Session
{
public:
	Session(QSharedPointer<Index> index, QSharedPointer<Metrics> metrics)
        : m_index(index), m_metrics(metrics) {}

    void begin();
    void commit();
    void rollback();
    void optimize();
    void cleanup();
    void insert(uint32_t id, const std::vector<uint32_t> &hashes);
    std::vector<SearchResult> search(const std::vector<uint32_t> &hashes);

    QString getAttribute(const QString &name);
    void setAttribute(const QString &name, const QString &value);

    int64_t getTimeout() const { return m_timeout; }
    int64_t getIdleTimeout() const { return m_idle_timeout; }

    QSharedPointer<Metrics> metrics() const { return m_metrics; }

    QString getTraceId();
    void clearTraceId();

private:
	QMutex m_mutex;
    QSharedPointer<Index> m_index;
    QSharedPointer<IndexWriter> m_indexWriter;
    QSharedPointer<Metrics> m_metrics;
	int m_topScorePercent { 10 };
	int m_maxResults { 500 };
    int64_t m_timeout { 0 };
    int64_t m_idle_timeout { 60 * 1000 };
    QString m_traceId;
};

}
}

#endif
