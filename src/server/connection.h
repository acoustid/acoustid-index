// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SERVER_CONNECTION_H_
#define ACOUSTID_SERVER_CONNECTION_H_

#include <QByteArray>
#include <QTcpSocket>
#include <QTimer>
#include <QSharedPointer>
#include <QPointer>
#include <QFutureWatcher>
#include "index/index.h"
#include "index/index_writer.h"
#include "request.h"

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
	void sendResponse(const QSharedPointer<Request> &request, const QString& response);
	void readIncomingData();
    void resetIdleTimeoutTimer();

signals:
	void disconnected();

private:
	QString m_client;
	QTcpSocket *m_socket;
    QByteArray m_buffer;
    QSharedPointer<Session> m_session;
    QFutureWatcher<QPair<QSharedPointer<Request>, QString>> *m_handler;
    QTimer *m_idle_timeout_timer;
    QSharedPointer<Request> m_active_request;
};

}
}

#endif

