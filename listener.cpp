#include <QDebug>
#include <QCoreApplication>
#include "listener.h"
#include "connection.h"

Listener::Listener(QObject *parent)
	: QTcpServer(parent)
{
	connect(this, SIGNAL(newConnection()), SLOT(acceptNewConnection()));
}

Listener::~Listener()
{
}

void Listener::stop()
{
	qDebug() << "Stopping" << this;
	connect(this, SIGNAL(lastConnectionClosed()), qApp, SLOT(quit()));
	close();
	foreach (Connection *connection, m_connections) {
		connection->close();
	}
}

void Listener::removeConnection(Connection *connection)
{
	qDebug() << "Removing connection" << connection;
	m_connections.removeAll(connection);
	if (m_connections.isEmpty()) {
		emit lastConnectionClosed();
	}
}

void Listener::acceptNewConnection()
{
	QTcpSocket *socket = nextPendingConnection();
	Connection *connection = new Connection(socket, this);
	m_connections.append(connection);
	qDebug() << "Adding connection" << connection;
	connect(connection, SIGNAL(closed(Connection *)), SLOT(removeConnection(Connection *)));
}

