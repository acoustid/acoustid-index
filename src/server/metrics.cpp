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

void Metrics::onSearchRequest(double duration, int resultCount) {
	QWriteLocker locker(&m_lock);
	m_searchRequestCount += 1;
	m_searchRequestDurationSum += duration;
	if (resultCount > 0) {
		m_searchHitCount += 1;
	} else {
		m_searchMissCount += 1;
	}
}

void Metrics::onInsertRequest(double duration) {
	QWriteLocker locker(&m_lock);
	m_insertRequestCount += 1;
	m_insertRequestDurationSum += duration;
}

QStringList Metrics::toStringList() {
	QReadLocker locker(&m_lock);
	QStringList output;
	output.append(QString("# TYPE connections_in_flight gauge"));
	output.append(QString("connections_in_flight %1").arg(m_connectionInFlightCount));
	output.append(QString("# TYPE connections_total counter"));
	output.append(QString("connections_total %1").arg(m_connectionCount));
	output.append(QString("# TYPE requests_total counter"));
	output.append(QString("requests_total{operation=\"search\"} %1").arg(m_searchRequestCount));
	output.append(QString("requests_total{operation=\"insert\"} %1").arg(m_insertRequestCount));
	output.append(QString("# TYPE requests_duration_seconds counter"));
	output.append(QString("requests_duration_seconds{operation=\"search\"} %1").arg(m_searchRequestDurationSum));
	output.append(QString("requests_duration_seconds{operation=\"insert\"} %1").arg(m_insertRequestDurationSum));
	return output;
}
