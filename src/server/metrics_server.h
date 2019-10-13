// Copyright (C) 2019  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SERVER_METRICS_SERVER_H_
#define ACOUSTID_SERVER_METRICS_SERVER_H_

#include <QTcpServer>
#include <QTcpSocket>
#include "metrics.h"

namespace Acoustid {
namespace Server {

class MetricsServerConnection : public QObject
{
	Q_OBJECT

public:
	MetricsServerConnection(QTcpSocket *socket, QObject *parent = nullptr);
	~MetricsServerConnection();

	void close();

signals:
	void closed();

private slots:
	void handleDisconnect();
	void readData();
	void writeResponse(const QString &httpVersion, const QString &status, const QString &responseBody);

private:
	QTcpSocket *m_socket;
	QByteArray m_buffer;
};

class MetricsServer : public QObject
{
	Q_OBJECT

public:
	MetricsServer(QSharedPointer<Metrics> metrics, QObject *parent = nullptr);
	~MetricsServer();

	void start(const QHostAddress &address = QHostAddress::Any, quint16 port = 0);
	void stop();

	QSharedPointer<Metrics> metrics() const;

protected slots:
	void acceptNewConnection();

private:
	QTcpServer *m_listener;
	QSharedPointer<Metrics> m_metrics;
};

}
}

#endif
