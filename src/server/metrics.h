// Copyright (C) 2019  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SERVER_METRICS_H_
#define ACOUSTID_SERVER_METRICS_H_

#include <QReadWriteLock>
#include "index/index.h"
#include "store/directory.h"

namespace Acoustid {
namespace Server {

class Metrics
{
public:
	Metrics();
	~Metrics();

	void onNewConnection();
	void onClosedConnection();

	void onRequest(const QString &name, double duration);
	void onSearchRequest(int resultCount);

	QStringList toStringList();

private:
	QReadWriteLock m_lock;

	uint64_t m_connectionInFlightCount { 0 };
	uint64_t m_connectionCount { 0 };

	QMap<QString, uint64_t> m_requestCount;
	QMap<QString, double> m_requestDurationSum;

	uint64_t m_searchHitCount { 0 };
	uint64_t m_searchMissCount { 0 };
};

}
}

#endif
