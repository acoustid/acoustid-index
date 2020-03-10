// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SERVER_CONNECTION_H_
#define ACOUSTID_SERVER_CONNECTION_H_

#include <QTextStream>
#include <QByteArray>
#include <QTcpSocket>
#include <QSharedPointer>
#include <QPointer>
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
	void close();

protected:
	void sendResponse(const QString& response, bool next = true);
	void handleLine(const QString& line);

signals:
	void closed(Connection *connection);

protected slots:
	void readIncomingData();
	void sendHandlerResponse(const QString& response);

	void onDisconnect();

private:
	QString m_client;
	QTcpSocket *m_socket;
	QString m_buffer;
	QTextStream m_output;
    QSharedPointer<Session> m_session;
	QPointer<Handler> m_handler;
	int m_topScorePercent;
	int m_maxResults;
	int m_refs;
};

}
}

#endif

