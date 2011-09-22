// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SERVER_HANDLER_H_
#define ACOUSTID_SERVER_HANDLER_H_

#include <QRunnable>
#include <QObject>
#include <QStringList>
#include "util/exceptions.h"
#include "connection.h"

namespace Acoustid {

class Index;

namespace Server {

class HandlerException : public Exception
{
public:
	HandlerException(const QString& msg) : Exception(msg) { }
};

#define ACOUSTID_HANDLER_CONSTRUCTOR(x) \
	x(Connection* connection, const QStringList& args) : Handler(connection, args) { }

class Handler : public QObject, public QRunnable
{
	Q_OBJECT

public:
	Handler(Connection* connection, const QStringList& args);
	virtual ~Handler();

	virtual void run();
	virtual QString handle() = 0;

	Connection* connection() { return m_connection; }
	IndexSharedPtr index() { return m_connection->index(); }
	QStringList args() { return m_args; }

signals:
	void finished(QString result);

private:
	Connection* m_connection;
	QStringList m_args;
};

}
}

#endif

