// Copyright (C) 2019  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "metrics_server.h"

namespace Acoustid {
namespace Server {

static const QByteArray kCRLF { "\r\n", 2 };

MetricsServer::MetricsServer(QSharedPointer<Metrics> metrics, QObject *parent)
	: QObject(parent), m_listener(new QTcpServer(this)), m_metrics(metrics)
{
	connect(m_listener, SIGNAL(newConnection()), SLOT(acceptNewConnection()));
}

MetricsServer::~MetricsServer()
{
}

QSharedPointer<Metrics> MetricsServer::metrics() const
{
	return m_metrics;
}

void MetricsServer::start(const QHostAddress &address, quint16 port)
{
	m_listener->listen(address, port);
}

void MetricsServer::stop()
{
	m_listener->close();
}

void MetricsServer::acceptNewConnection()
{
	auto socket = m_listener->nextPendingConnection();
	auto connection = new MetricsServerConnection(socket, this);

}

MetricsServerConnection::MetricsServerConnection(QTcpSocket *socket, QObject *parent)
	: QObject(parent), m_socket(socket)
{
	m_socket->setParent(this);
	connect(m_socket, SIGNAL(readyRead()), SLOT(readData()));
	connect(m_socket, SIGNAL(disconnected()), SLOT(handleDisconnect()));
}

MetricsServerConnection::~MetricsServerConnection()
{
}

void MetricsServerConnection::close()
{
	m_socket->disconnectFromHost();
}

void MetricsServerConnection::writeResponse(const QString &httpVersion, const QString &status, const QString &responseBody)
{
	auto responseBodyBytes = responseBody.toUtf8();

	QString responseHeader;
	responseHeader += QString("%1 %2").arg(httpVersion, status) + kCRLF;
	responseHeader += QString("Content-Type: text/plain; version=0.0.4; charset=utf-8") + kCRLF;
	responseHeader += QString("Content-Length: %1").arg(responseBodyBytes.size()) + kCRLF;
	responseHeader += QString("Connection: close") + kCRLF;
	responseHeader += kCRLF;
	auto responseHeaderBytes = responseHeader.toAscii();

	m_socket->write(responseHeaderBytes);
	m_socket->write(responseBodyBytes);
}

void MetricsServerConnection::readData()
{
	m_buffer += m_socket->readAll();
	auto pos = m_buffer.indexOf(kCRLF + kCRLF);
	if (pos == -1) {
		return;
	}

	auto requestHeaderBytes = m_buffer.left(pos);
	m_buffer = m_buffer.mid(pos + 4);

	auto requestHeader = QString::fromAscii(requestHeaderBytes);
	auto requestHeaderLines = requestHeader.split(kCRLF);

	if (requestHeaderLines.isEmpty()) {
		close();
		return;
	}

	auto requestLineParts = requestHeaderLines.at(0).split(" ");
	if (requestLineParts.size() != 3) {
		close();
		return;
	}

	auto httpVersion = requestLineParts.at(2);
	if (!httpVersion.startsWith("HTTP/1.")) {
		close();
		return;
	}

	if (requestLineParts.at(0) != "GET") {
		writeResponse(httpVersion, "405 Method Not Allowed", "");
		return;
	}

	if (requestLineParts.at(1) != "/metrics") {
		writeResponse(httpVersion, "404 Not Found", "");
		return;
	}

	auto server = qobject_cast<MetricsServer *>(parent());
	writeResponse(httpVersion, "200 OK", server->metrics()->toStringList().join("\n") + "\n");
}

void MetricsServerConnection::handleDisconnect()
{
	emit closed();
	deleteLater();
}

}
}
