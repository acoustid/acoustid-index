// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "common.h"
#include "handler.h"
#include <QElapsedTimer>

using namespace Acoustid;
using namespace Acoustid::Server;

inline double elapsedSeconds(const QElapsedTimer& timer) {
	return timer.elapsed() / 1000.0;
}

Handler::Handler(Connection* connection, const QString &name, const QStringList& args)
	: m_connection(connection), m_name(name), m_args(args)
{
}

Handler::~Handler()
{
}

void Handler::run()
{
	QElapsedTimer timer;
	timer.start();
	QMutexLocker locker(m_connection->mutex());
	QString result;
	try {
		result = QString("OK %1").arg(handle());
	}
	catch (HandlerException& ex) {
		result = QString("ERR %1").arg(ex.what());
	}
	catch (Exception& ex) {
		qCritical() << "Unexpected exception in handler" << ex.what();
		result = QString("ERR %1").arg(ex.what());
	}
	emit finished(result);
	metrics()->onRequest(m_name, elapsedSeconds(timer));
}

