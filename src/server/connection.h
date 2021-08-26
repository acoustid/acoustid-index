// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SERVER_CONNECTION_H_
#define ACOUSTID_SERVER_CONNECTION_H_

#include <QTextStream>
#include <QByteArray>
#include <QTcpSocket>
#include <QSharedPointer>
#include <QPointer>
#include <QFutureWatcher>
#include "index/index.h"
#include "index/index_writer.h"

namespace Acoustid {
namespace Server {

class Listener;
class Handler;
class Session;

class Connection : public QObject
{
	Q_OBJECT

public:
	Connection(IndexSharedPtr index, QTcpSocket *socket, QObject *parent = 0);
	~Connection();

	Listener *listener() const;
    QString client() const { return m_client; };

	void close();

protected:
	void sendResponse(const QString& response);
	void readIncomingData();

signals:
	void disconnected();

private:
	QString m_client;
	QTcpSocket *m_socket;
    QTextStream m_stream;
    QString m_line;
    QSharedPointer<Session> m_session;
    QFutureWatcher<QString> *m_handler;
};

}
}

#endif

