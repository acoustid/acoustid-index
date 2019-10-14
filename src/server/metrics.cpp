// Copyright (C) 2019  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "metrics.h"

using namespace Acoustid;
using namespace Acoustid::Server;

Metrics::Metrics()
{
}

Metrics::~Metrics()
{
}

void Metrics::onNewConnection() {
	QWriteLocker locker(&m_lock);
	m_connectionCount += 1;
	m_connectionInFlightCount += 1;
}

void Metrics::onClosedConnection() {
	QWriteLocker locker(&m_lock);
	m_connectionInFlightCount -= 1;
}

void Metrics::onSearchRequest(int resultCount) {
	QWriteLocker locker(&m_lock);
	if (resultCount > 0) {
		m_searchHitCount += 1;
	} else {
		m_searchMissCount += 1;
	}
}

void Metrics::onRequest(const QString &name, double duration) {
	QWriteLocker locker(&m_lock);
	m_requestCount[name] += 1;
	m_requestDurationSum[name] += duration;
}

QStringList Metrics::toStringList() {
	QReadLocker locker(&m_lock);
	QStringList output;

	output.append(QString("# TYPE aindex_connections_in_flight gauge"));
	output.append(QString("aindex_connections_in_flight %1").arg(m_connectionInFlightCount));

	output.append(QString("# TYPE aindex_connections_total counter"));
	output.append(QString("aindex_connections_total %1").arg(m_connectionCount));

	output.append(QString("# TYPE aindex_requests_total counter"));
	{
		const auto iter = m_requestCount.constBegin();
		while (iter != m_requestCount.constEnd()) {
			output.append(QString("aindex_requests_total{operation=\"%1\"} %2").arg(iter.key(), iter.value()));
		}
	}

	output.append(QString("# TYPE aindex_requests_duration_seconds counter"));
	{
		const auto iter = m_requestDurationSum.constBegin();
		while (iter != m_requestDurationSum.constEnd()) {
			output.append(QString("aindex_requests_duration_seconds{operation=\"%1\"} %2").arg(iter.key(), iter.value()));
		}
	}

	output.append(QString("# TYPE aindex_search_hits_total counter"));
	output.append(QString("aindex_search_hits_total %1").arg(m_searchHitCount));

	output.append(QString("# TYPE aindex_search_misses_total counter"));
	output.append(QString("aindex_search_misses_total %1").arg(m_searchMissCount));

	return output;
}
