// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SERVER_HANDLER_H_
#define ACOUSTID_SERVER_HANDLER_H_

#include <QRunnable>
#include <QObject>
#include <QStringList>
#include "util/exceptions.h"
#include "connection.h"
#include "metrics.h"
#include "listener.h"

namespace Acoustid {

class Index;

namespace Server {

class Metrics;

class HandlerException : public Exception
{
public:
	HandlerException(const QString& msg) : Exception(msg) { }
};

#define ACOUSTID_HANDLER_CONSTRUCTOR(x) \
	x(Connection* connection, const QString &name, const QStringList& args) : Handler(connection, name, args) { }

class Handler : public QObject, public QRunnable
{
	Q_OBJECT

public:
	Handler(Connection* connection, const QString &name, const QStringList& args);
	virtual ~Handler();

	virtual void run();
	virtual QString handle() = 0;

	Connection* connection() { return m_connection; }
	IndexSharedPtr index() { return m_connection->index(); }
	QSharedPointer<Metrics> metrics() { return m_connection->listener()->metrics(); }
	QStringList args() { return m_args; }

signals:
	void finished(QString result);

private:
	Connection* m_connection;
    QString m_name;
	QStringList m_args;
};

}
}

#endif

