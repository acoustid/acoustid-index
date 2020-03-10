// Copyright (C) 2020  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SERVER_SESSION_H_
#define ACOUSTID_SERVER_SESSION_H_

#include <QMutex>
#include <QSharedPointer>

namespace Acoustid {

class Index;

namespace Server {

class Metrics;

class Session
{
public:
	Session(QSharedPointer<Index> index, QSharedPointer<Metrics> metrics)
        : m_index(index), m_metrics(metrics) {}

	QSharedPointer<Index> index() { return m_index; }
	QSharedPointer<IndexWriter> indexWriter() { return m_indexWriter; }
	QSharedPointer<Metrics> metrics() { return m_metrics; }

	void setIndexWriter(QSharedPointer<IndexWriter> indexWriter)
	{
		m_indexWriter = indexWriter;
	}

	QMutex* mutex()
	{
		return &m_mutex;
	}

private:
	QMutex m_mutex;
    QSharedPointer<Index> m_index;
    QSharedPointer<IndexWriter> m_indexWriter;
    QSharedPointer<Metrics> m_metrics;
};

}
}

#endif
